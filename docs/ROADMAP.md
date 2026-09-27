# Roadmap and acceptance cursor

## Accepted foundation

- [x] typed protocol and encrypted mailbox transport;
- [x] hosted Linux/Windows CI;
- [x] verified Windows bundle deployment;
- [x] one TimeTrigger controller supervisor;
- [x] one Rust agent supervisor with Job Object ownership;
- [x] controller crash -> automatic scheduler recovery;
- [x] agent recovery;
- [x] encrypted analytical transport after recovery.

## Product stages

### M1 Reference Data Registry — PASS

Authority: #23

- public instruments bootstrap;
- normalized InstrumentSpec;
- deterministic reference generation;
- encrypted InstrumentRules physical proof;
- post-restart deterministic rebuild.

### M2 Public REST Market State — PASS

Authority: #26

- ticker/bid/ask;
- mark/index;
- funding;
- open interest;
- deterministic market generation;
- request-time fresh REST bootstrap;
- explicit `DEGRADED / M2_REST_BOOTSTRAP_ONLY`;
- encrypted Windows physical acceptance.

### M3 Persistent Public OKX WebSocket — PASS

Authority: #30

Accepted:
- `okx-ws` protocol/transport boundary only;
- one `okx-runtime::PublicWsCoordinator` lifecycle owner;
- REST bootstrap + WS convergence;
- live instruments/ticker/mark/index/funding/OI state;
- Decimal-backed order-book state;
- strict `seqId/prevSeqId` continuity;
- evidence-derived NOT_READY/DEGRADED/FRESH/STALE;
- encrypted physical FRESH SnapshotQuality + MarketSnapshot;
- agent restart revokes old evidence and rebuilds FRESH deterministically.

The disruptive physical forced-network-loss proof is DEFERRED with #16 R3, not PASS.

Checksum validation is explicitly forbidden for current OKX JSON order-book channels because OKX deprecated it in production on 2026-06-23.

### Post-M3 runtime readability cleanup — PASS

Authority: #42

Module-split `okx-runtime::public` inside the same crate. No behavior, schema or lifecycle-owner change. Accepted via PR #45; #42 CLOSED/PASS.

### Q0 Current-environment query/transport efficiency — CURRENT

Authority: #47

Before M4, harden the existing encrypted GitHub mailbox instead of introducing another transport:

- incremental/high-water mailbox cursor instead of full-history polling;
- bounded mailbox transport metrics;
- typed instrument discovery;
- composite MarketOverview query;
- retention/rotation/compaction policy with replay safety;
- physical DATA #10 acceptance after migration.

### M4 Private read-only Account + Order State — NEXT

- balances/equity;
- positions;
- account configuration;
- pending orders/fills;
- REST bootstrap + private WS convergence;
- observer credential only.

### M5 Deterministic Analysis

Planned after M4.

- Decimal/fixed-point fees/cost;
- risk/exposure;
- sizing;
- stop/TP;
- hypothetical scenarios.

### Access transport decision

No MCP stage is planned. The encrypted GitHub mailbox is the normal access path in the current Windows/GitHub environment. Future transport work must improve this path incrementally rather than introduce a second access stack.

## Explicit deferred lifecycle debt

Authority: #16

Not PASS:

- [ ] R3 external network/GitHub-loss physical recovery;
- [x] R4 Windows reboot + sign-in physical recovery — PASS;
- [ ] final no-duplicate count associated with those disruptive tests.

This debt is intentionally visible. It does not invalidate the completed non-disruptive M3 acceptance, but remains open before final lifecycle closure.

## Non-goals for M1–M6

- no live order mutation;
- no autonomous strategy execution;
- no withdrawals/transfers;
- no executor credential in observation;
- no second lifecycle supervisor;
- no local Windows production build in the normal deployment path.

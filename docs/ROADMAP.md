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

### M3 Persistent Public OKX WebSocket — CURRENT

Authority: #32

Target:

1. one public WS lifecycle owner;
2. typed connect/subscribe/event/error/notice handling;
3. bounded reconnect with explicit WS generation;
4. REST bootstrap + WS convergence;
5. live instruments/ticker/mark/index/funding/OI state;
6. order-book snapshot + incremental apply;
7. strict `seqId/prevSeqId` continuity;
8. readiness transitions;
9. `FRESH` only after complete proof;
10. physical disconnect/reconnect acceptance.

Checksum validation is explicitly forbidden for current OKX JSON order-book channels because OKX deprecated it in production on 2026-06-23.

### M4 Private read-only Account + Order State

Planned after M3.

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

### M6 MCP

Planned after stable typed Query API.

MCP becomes the normal ChatGPT access adapter. The encrypted GitHub mailbox remains fallback/diagnostic transport.

## Explicit deferred lifecycle debt

Authority: #16

Not PASS:

- [ ] R3 external network/GitHub-loss physical recovery;
- [ ] R4 Windows reboot + sign-in physical recovery;
- [ ] final no-duplicate count associated with those disruptive tests.

This debt is intentionally visible but does not block M3 product development.

## Non-goals for M1–M6

- no live order mutation;
- no autonomous strategy execution;
- no withdrawals/transfers;
- no executor credential in observation;
- no second lifecycle supervisor;
- no local Windows production build in the normal deployment path.

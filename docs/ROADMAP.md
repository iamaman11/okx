# Roadmap and acceptance cursor

## Accepted foundation

- [x] typed protocol and encrypted mailbox transport;
- [x] hosted Linux/Windows CI;
- [x] verified Windows bundle deployment;
- [x] one TimeTrigger controller supervisor;
- [x] one Rust host-control supervisor with Job Object ownership;
- [x] controller crash -> automatic scheduler recovery;
- [x] agent recovery;
- [x] Windows reboot/sign-in recovery;
- [x] real external network/GitHub-loss -> asynchronous CONTROL/DATA recovery;
- [x] exact installed-agent provenance;
- [x] H1 conditional GitHub polling / failure classification / response budgets / compact evidence.

## Read-only product — CLOSED/PASS

### M1 Reference Data Registry — PASS

Authority: #23.

- public instruments bootstrap;
- normalized InstrumentSpec;
- deterministic reference generation;
- encrypted InstrumentRules physical proof;
- post-restart deterministic rebuild.

### M2 Public REST Market State — PASS

Authority: #26.

- ticker/bid/ask;
- mark/index;
- funding;
- open interest;
- deterministic market generation;
- explicit quality.

### M3 Persistent Public OKX WebSocket — PASS

Authority: #30.

- one public WS lifecycle owner;
- REST bootstrap/recovery + WS live ownership;
- demand-driven subscriptions;
- seqId/prevSeqId order-book continuity;
- generation-bound readiness;
- restart and real network-loss recovery accepted.

Checksum is not an integrity authority for the current OKX order-book channels.

### Q0 / M4 / M5 / M6 — PASS

- Q0 query/transport efficiency — #47 CLOSED/PASS;
- M4 private read-only account/order state — #59 CLOSED/PASS;
- M5 deterministic Decimal cost/risk/scenario analysis — #65 CLOSED/PASS;
- M6 HistoryBehavior / PositionScenario / CurrentCost / bounded MarketResearch — #72 CLOSED/PASS;
- H1 GitHub transport, response-shaping and context-efficiency — #83 CLOSED/PASS;
- A2 unattended Windows lifecycle / reboot / external-network recovery — #16 CLOSED/PASS.

The current read-only platform is production-accepted in the present Windows + GitHub environment.

## Current large slice — Phase 2 production execution boundary

Authority: #3.

Goal: build a production-grade mutation boundary while keeping live trading disabled.

The existing observation/analysis stack remains unchanged and read-only. Phase 2 introduces exactly one mutation owner and must not turn the query layer, GitHub transport, observation state or analysis crates into trading engines.

Required foundation:

- separate executor credential custody: Read + Trade, never Withdraw;
- executor credential is not the observer credential and is not used on GitHub-hosted runners;
- executor identity must be proven to target the same intended sub-account without exposing raw UID;
- one OrderExecutor owns place/amend/cancel/close mutation sequencing;
- typed immutable execution intents only;
- deterministic globally unique clOrdId policy;
- persisted mutation ledger across restart;
- instrument/reference generation validation;
- tick/lot/min/max and account-position-mode validation;
- explicit tdMode, side, posSide, order type and reduce-only semantics;
- server-time / expiry policy before writes;
- ACK is not fill confirmation;
- lost/ambiguous submission becomes UNKNOWN_SUBMISSION;
- UNKNOWN_SUBMISSION must reconcile by clOrdId before any retry;
- private orders WS remains observation evidence, not mutation authority;
- batch partial success must be represented per item;
- all public/hosted acceptance before explicit enablement is non-mutating;
- ALLOW_LIVE_TRADING=false remains the hard production default.

Phase 2 is one large logical slice with internal hosted and physical gates, not a new chain of alphabetic micro-stages.

## Deferred, non-blocking maintenance

### Controller self-update — #54

Deferred unless controller upgrades become frequent enough to justify the extra lifecycle complexity. Normal operation is already remotely controlled.

### DATA mailbox compaction — #58

Deferred until the existing capacity trigger or real recovery evidence requires it. No speculative compactor or second transport owner.

## Access transport

No MCP migration is planned. The encrypted GitHub mailbox remains the current typed access path. Future execution intents, if exposed through it, must remain encrypted, strictly typed and fail-closed.

## Security non-goals

- no withdrawal/transfer API;
- no arbitrary shell/HTTP proxy;
- no generic batch/DSL/expression engine;
- no autonomous strategy engine in the execution-boundary slice;
- no live order mutation until Phase 2 acceptance explicitly authorizes it;
- no second lifecycle supervisor;
- no local Windows production build in the normal deployment path.

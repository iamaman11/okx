# Architecture

## Principles

The platform must remain simple, layered, modular and fail-closed.

One owner exists for every lifecycle/state boundary. A recovery mechanism may retry its own responsibility, but no second watchdog or duplicated state machine may compete for ownership.

## Planes

```text
DATA PLANE
OKX Public WS/REST + Private WS/REST
  -> typed exchange adapters
  -> normalized observation state
  -> reconciliation/readiness
  -> immutable snapshots

ANALYSIS PLANE
immutable snapshot
  -> cost
  -> risk
  -> scenario

EXECUTION PLANE — Phase 2
typed immutable execution intent
  -> one OrderExecutor
  -> validation + idempotency ledger
  -> OKX trade mutation boundary
  -> reconcile ACK/order state through authoritative exchange evidence

ACCESS PLANE
ChatGPT / CLI / UI
  -> typed Query API
  -> encrypted GitHub mailbox
  -> canonical current access path
```

## Crate boundaries

### okx-api

Exchange boundary only:

- REST request/response DTOs;
- public and private typed primitives;
- authentication/signing where required;
- no normalized domain ownership;
- no business/risk logic.

### okx-observation

Domain state owner:

- Reference Registry;
- Market State;
- Account and Order State;
- generation IDs;
- reconciliation;
- readiness;
- immutable snapshots.

### okx-ws

WebSocket protocol/transport boundary only:

- TLS/WebSocket connect and frame transport;
- typed subscribe/unsubscribe and channel envelopes;
- application `ping` / typed `pong` primitives;
- no reconnect timer, generation ownership or domain state.

### okx-runtime

One Tokio observation lifecycle owner:

- public WebSocket reconnect/backoff;
- connection generation;
- desired-vs-observed subscriptions;
- application heartbeat timing;
- REST bootstrap + WS convergence orchestration.

### okx-execution

Single mutation owner only:

- immutable validated ExecutionPlan;
- deterministic client order id;
- durable bounded execution ledger;
- PREPARED/SUBMITTING/ACKNOWLEDGED/UNKNOWN_SUBMISSION/exchange-state reconciliation;
- typed gateway to okx-api trade primitives;
- no observation ownership;
- no analytical calculations;
- production construction remains live-trading disabled until explicit pre-enable acceptance.

### okx-agent

Composition root and access adapter only. It starts/owns the runtime as a component but must not duplicate its lifecycle state machine, domain calculations, execution state or Windows supervision.

### Runtime maintainability rule

Large lifecycle modules may be split by responsibility **inside the same crate** when readability degrades. A file/module split must never create another lifecycle owner. Post-M3 cleanup #42 applies this rule to `okx-runtime::public` before M4.

### okx-host-control

Windows lifecycle/deployment/diagnostics only:

- desired RUNNING/STOPPED;
- one child process;
- Job Object ownership;
- verified artifact deployment;
- bounded child restart.

No OKX market/account/risk logic belongs here.

## Windows supervision

```text
Task Scheduler
  ONE TimeTrigger / PT1M indefinite
  StartWhenAvailable=true
  IgnoreNew
        |
        v
ONE okx-host-control
  mutex
  desired.json
  Job Object / KILL_ON_JOB_CLOSE
  bounded child restart
        |
        v
ONE okx-agent
```

Task Scheduler supervises the controller only. The Rust controller supervises the agent only.

## Windows filesystem boundaries

```text
C:\okx          mutable canonical Git workspace
C:\okx-control  installed controller + desired lifecycle state
C:\okx-runtime  installed agent + runtime logs/staging
```

These are deliberately separate trust/lifecycle boundaries. Installed binaries must not execute from the mutable repository checkout.

`C:\okx-upgrade` is not a canonical boundary. It is temporary staging left from an earlier one-time manual controller upgrade and is not referenced by normal Scheduler/controller/agent operation.

## Market-data evolution

### M1 — Reference Data

Public REST instruments bootstrap -> normalized deterministic Reference Registry.

Accepted and closed.

### M2 — REST Market State

Fresh finite public REST calls produce a coherent immutable market snapshot:

- ticker/bid/ask;
- mark price;
- index price;
- funding;
- open interest.

M2 remains `DEGRADED` by design.

Accepted and closed.

### M3 — Persistent Public WebSocket

WebSocket becomes the live market-state owner. REST remains bootstrap/verification/recovery.

Required evidence before `FRESH`:

- live connection generation;
- required subscriptions acknowledged;
- current Reference generation;
- required source updates;
- order-book initial snapshot;
- `seqId/prevSeqId` continuity;
- no unresolved gap/reconciliation conflict.

A connected socket alone is never readiness.

M3/A2 are physically accepted, including real external network/GitHub loss and asynchronous CONTROL/DATA recovery without duplicate runtime owners.

## Order-book integrity

As of OKX production changes effective 2026-06-23, the `checksum` field for `books`, `books-l2-tbt` and `books50-l2-tbt` is deprecated and fixed to `0`.

The runtime must use only documented `seqId/prevSeqId` rules for continuity.

No new code may introduce checksum-based integrity authority.

## Security

- observation runtime is read-only;
- private state later uses observer credentials only;
- execution credentials remain outside observation;
- no arbitrary shell/HTTP proxy operation;
- no raw credentials or private plaintext in public GitHub;
- analysis consumes immutable snapshots, never mutable collectors directly.

## Application query boundary

The product has two query levels over the same authorities.

**Level 1 — forensic/detail** exposes bounded factual snapshots for drill-down and diagnostics:
`InstrumentRules`, `FindInstruments`, `MarketSnapshot`, `MarketOverview`, `MarketHistory`,
`HistoryBehavior`, `SnapshotQuality`, `AccountSnapshot`, and transport telemetry.

**Level 2 — application/research** answers a demonstrated research question by composing immutable
inputs locally and returning compact attributable evidence:
`MarketResearch`, `PortfolioRisk`, `CurrentCost`, `PositionScenario`, and
`AnalyzeCandidateOrder`.

The admission rule is:

> If ChatGPT would otherwise fetch a large raw dataset and repeat deterministic domain arithmetic,
> aggregation, filtering, consistency checks, or bounded scenario expansion, that work belongs in
> a typed Level 2 operation next to the immutable inputs.

Level 2 does **not** introduce a new engine. Existing ownership remains unchanged:
observation owns factual state, `okx-analysis` owns deterministic Decimal math, and query adapters
only acquire bounded dependencies, enforce consistency, call pure analysis, and project results.

Every Level 2 operation must have:

- explicit bounded request shape and work limits;
- operation-scoped immutable dependency acquisition, with each exact dependency captured once and
  reused within that request;
- explicit quality and generation-consistency rules that never upgrade source quality;
- compact result provenance sufficient to avoid implying atomic simultaneity;
- an operation-specific response budget before the GitHub envelope boundary;
- no arbitrary batch, expression language, field-selection DSL, local LLM, SQL layer, or new
  long-lived state owner.

Current MarketResearch work budget is `2..=8` unique instruments with history `limit <= 100`.
Each instrument is assembled from one current-market acquisition plus one bounded history
acquisition, then existing pure `analyze_history_behavior` performs deterministic reduction.
The v2 projection retains per-instrument market/history timestamps and generations while omitting
full nested forensic snapshots. Level 1 operations remain available when raw/detail evidence is
explicitly required.


## Accepted platform cursor — 2026-09-28

The read-only platform is fully accepted through M6, H1 and A2:

- M1 reference data — PASS;
- M2 public REST market state — PASS;
- M3 persistent public WS — PASS;
- M4 private account/orders — PASS;
- M5 deterministic analysis — PASS;
- M6 bounded application/research queries — PASS;
- H1 transport/context hardening — PASS;
- A2 reboot + real external network-loss recovery — PASS.

The next large product boundary is Phase 2 (#3). It must preserve every existing observation/analysis ownership rule.

## Phase 2 execution ownership

Execution is intentionally separate from observation and analysis.

```text
immutable accepted inputs
  Reference / Market / Account / Analysis
                |
                v
        typed ExecutionIntent
                |
                v
          ONE OrderExecutor
      validation + durable ledger
                |
                v
       OKX trade mutation API
                |
                v
private orders/account observation
      -> reconciliation evidence
```

Rules:

- okx-api may contain typed OKX trade request/response primitives, but no mutation state machine;
- a dedicated execution component owns mutation sequencing, idempotency, UNKNOWN_SUBMISSION and reconciliation;
- observation never sends orders;
- analysis never sends orders;
- access/query adapters never directly call OKX mutation endpoints;
- executor credentials are distinct from observer credentials and never exposed to GitHub-hosted CI;
- clOrdId is generated by the execution owner and treated as a durable idempotency key even though OKX only enforces uniqueness among pending orders;
- an HTTP/WS ACK proves acceptance of a request, not a fill;
- network loss after submission must not cause blind resubmission;
- an ambiguous outcome is persisted as UNKNOWN_SUBMISSION and reconciled by exact clOrdId / exchange order evidence before further mutation;
- long/short account mode requires explicit valid side + posSide combinations;
- reference generation, tick/lot/min and account-mode assumptions are revalidated at the mutation boundary;
- live writes remain disabled until explicit Phase-2 production acceptance changes the gate.

Accepted Phase-2 cursor:
- #96 pure execution model — PASS;
- #97 durable mutation ledger — PASS;
- #98 typed OKX mutation primitives — PASS;
- #99 one production-disabled OrderExecutor + UNKNOWN_SUBMISSION reconciliation — PASS.

Current work is credential/preflight, then disabled runtime integration. A prepared ExecutionPlan is not a timeless permit: immediately before any future send, the execution boundary must reacquire current authoritative reference/account state and require exact generation, identity, account-mode and trade-readiness continuity.

The first deployed Phase-2 runtime must be physically accepted with live mutation still impossible.

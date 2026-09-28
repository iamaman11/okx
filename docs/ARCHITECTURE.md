# Architecture

## Principles

The platform must remain simple, layered, modular and fail-closed.

One owner exists for every lifecycle/state boundary. A recovery mechanism may retry its own responsibility, but no second watchdog or duplicated state machine may compete for ownership.

## Planes

```text
DATA PLANE
OKX Public WS/REST + later Private WS/REST
  -> typed exchange adapters
  -> normalized observation state
  -> reconciliation/readiness
  -> immutable snapshots

ANALYSIS PLANE
immutable snapshot
  -> cost
  -> risk
  -> scenario

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
- later Account and Order State;
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

### okx-agent

Composition root and access adapter only. It starts/owns the runtime as a component but must not duplicate its lifecycle state machine, domain calculations or Windows supervision.

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

M3 is physically accepted for the non-disruptive path: FRESH encrypted quality/snapshot, advancing book sequence evidence, and fail-closed restart rebuild. The external network-loss proof remains deferred with #16 R3.

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


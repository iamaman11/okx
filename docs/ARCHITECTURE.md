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
  -> encrypted mailbox now
  -> MCP later
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

### okx-agent

Composition root and access adapter only.

It may own the long-lived Tokio observation tasks, but it must not duplicate domain calculations or Windows supervision.

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

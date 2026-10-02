# Architecture

## Principles

The platform is intentionally simple, layered, modular and fail-closed.

Core rules:

- one owner for every lifecycle/state boundary;
- no duplicate state machines or watchdogs;
- observation owns factual state;
- analysis owns deterministic calculations;
- execution owns all mutations;
- transport only transports;
- Cloudflare MCP is the primary ChatGPT data plane; GitHub encrypted DATA is fallback/parity only;
- Windows control only controls lifecycle/deployment;
- GitHub-hosted CI is the normal build authority;
- installed binaries are verified artifacts, not mutable-workspace builds;
- uncertain mutation outcome is reconciled, never blindly replayed.


## Repository architecture guard

The architecture is enforced by both code review and CI.

Objective repository invariants are encoded in `scripts/check_architecture.py`:
- the workspace crate set is explicit;
- local crate dependency directions are explicit;
- a new workspace crate or forbidden reverse dependency fails CI;
- the checker contains a negative self-test proving that an example reverse dependency is rejected.

CI keeps the guard cheap:
- architecture validation, portable Rust validation and Cloudflare MCP validation run in parallel;
- the already-required `linux-core` check only aggregates their results;
- architecture validation reads Cargo metadata and does not compile the workspace.

The PR template requires every nontrivial change to state:
- concrete need/failure;
- authoritative owner;
- smallest capability delta;
- structural architecture delta;
- invariant/failure-to-test mapping;
- superseded path/removal condition.

Structural complexity defaults to zero for new crates, long-lived tasks, state owners, stores, schedulers/poll loops, transports, mutation authorities, MCP tools, dependencies and durable schemas. A non-zero delta requires an explicit reason.

Machine checks intentionally do not enforce LOC/file-count/coverage/complexity scores. Those metrics would encourage code for the checker. Semantic simplicity is instead reviewed against ownership, duplication and current product need.

## Canonical topology

```text
                        SUPPLY CHAIN
GitHub PR
  -> pinned Actions + Rust 1.95.0 + committed Cargo.lock
  -> Linux / Windows CI
  -> exact Windows bundle
  -> manifest(source head/tree + binary SHA-256)
  -> verified deploy
  -> durable versioned recovery Release

                    PRIMARY CHATGPT DATA PLANE
ChatGPT
  -> OAuth MCP Worker
  -> ONE Durable Object rendezvous/correlation owner
  <- ONE outbound authenticated Windows WSS
        |
        v
ONE okx-agent / ONE Tokio runtime
        |
        +----------------------+--------------------+
        |                      |                    |
        v                      v                    v
 Observation             Pure Analysis       ONE OrderExecutor
 Reference/Market        Decimal math         durable ledger
 Account/Orders          scenario/risk        mutation authority
 reconciliation          no state owner       hard-disabled prod
 readiness
        |
        v
 immutable normalized facts/evidence

                   FALLBACK / LIFECYCLE PLANES
GitHub encrypted DATA #10 -> okx-agent        (fallback/parity only)
GitHub CONTROL #12        -> okx-host-control (deploy/lifecycle/recovery)

                       WINDOWS LIFECYCLE
Task Scheduler
  ONE TimeTrigger / PT1M / StartWhenAvailable / IgnoreNew
        |
        v
ONE fixed Scheduler entrypoint: C:\\okx-control\\okx-host-control.exe
  -> ONE versioned okx-host-control
  -> ONE Job-Object-owned okx-agent
```

## Crate boundaries

### `okx-api`

Exchange boundary only:

- typed REST/auth request/response primitives;
- public/private exchange DTOs;
- signing/authentication primitives;
- typed trade primitives;
- no normalized domain ownership;
- no mutation state machine;
- no business/risk policy.

### `okx-ws`

WebSocket protocol/transport only:

- TLS/WebSocket connection/framing;
- typed subscribe/unsubscribe/channel envelopes;
- application ping/pong primitives;
- no reconnect policy;
- no generation ownership;
- no normalized domain state.

### `okx-runtime`

Tokio observation lifecycle authority:

- public/private WebSocket coordination;
- reconnect/backoff;
- connection generation;
- desired-vs-observed subscriptions;
- heartbeat timing;
- REST bootstrap + WS convergence orchestration.

Module/file splits are allowed for readability but must never create another lifecycle owner.

### `okx-observation`

Factual domain-state authority:

- Reference Registry;
- Market State;
- Account and Order State;
- deterministic generations;
- reconciliation/readiness;
- immutable snapshots;
- explicit FRESH/STALE/DEGRADED/NOT_READY quality.

Observation never mutates exchange trading state.

### `okx-analysis`

Pure deterministic analysis:

- cost;
- fee-aware scenarios;
- portfolio/candidate risk;
- history behavior;
- current cost;
- position scenarios.

Inputs are immutable accepted snapshots. No collector, transport, lifecycle or mutation ownership lives here.

### `okx-execution`

Sole mutation authority:

- immutable validated ExecutionPlan;
- deterministic client order identity;
- durable bounded execution ledger;
- PREPARED / SUBMITTING / ACKNOWLEDGED / UNKNOWN_SUBMISSION and terminal/exchange reconciliation;
- exhaustive deterministic prepare classification;
- exact reconciliation by immutable plan identity;
- typed gateway to `okx-api` mutation primitives;
- compact read-only ExecutionStatus;
- no observation ownership;
- no analytical ownership.

Production construction is hard-disabled before SUBMITTING persistence and before exchange send. There is no accepted production live-write enable mechanism.

### `okx-agent`

Composition root and typed access adapter:

- wires observation/runtime/analysis/execution components;
- processes authenticated encrypted DATA requests;
- assembles bounded query dependencies;
- projects compact typed results.

It does not duplicate observation/runtime/execution state machines.

### `okx-protocol`

Versioned DATA/CONTROL contracts only.

### `cloudflare/okx-mcp`

Primary ChatGPT transport adapter only:

- OAuth/authz;
- MCP schema and argument validation;
- one Durable Object for Windows WSS rendezvous, freshness and request correlation;
- thin mapping from a small public tool surface to versioned `AgentOperation` contracts;
- bounded request/response deadlines and payloads;
- no OKX client, financial formula, account/market authority, execution policy or duplicate business state.

A Worker deploy may interrupt the direct session transiently; the Windows-owned outbound reconnect path restores a fresh generation. Socket existence alone is never liveness.

### `okx-bridge-mcp` (legacy/superseded)

This GitHub-backed MCP adapter predates the accepted Cloudflare-primary path. It is **not** a product authority and must receive no new product capabilities. Keep it only while a concrete deployment/recovery dependency still exists; otherwise remove it from the workspace and architecture allowlist rather than maintaining two MCP implementations.

### `okx-github`

GitHub transport primitives only:

- pinned repository/user identity;
- bounded issue retrieval;
- persisted cursors/terminal replay metadata;
- failure classification/backoff.

GitHub is transport/evidence, not market/account state authority.

### `okx-host-launcher`

Immutable Windows controller installation root-of-trust only:

- one-shot process started by the one Scheduler task;
- content-addressed controller versions;
- durable active/staged/pending/ready/result records under `C:\okx-control`;
- exact SHA-256 verification;
- exact Windows process identity (PID + process creation time) for old-controller wait and readiness proof;
- readiness event plus durable ready record;
- deterministic commit/rollback across crash/reboot, including PID reuse;
- no GitHub, OKX, mailbox, trading, workspace or agent business semantics;
- no daemon/watchdog/polling loop.

### `okx-host-control`

Windows lifecycle/deployment/diagnostics only:

- desired RUNNING/STOPPED;
- exactly one Job-Object-owned child agent;
- bounded restart;
- verified artifact deployment;
- Scheduler/autostart diagnostics;
- typed allowlisted CONTROL operations;
- bounded Windows workspace diagnostics;
- verified controller staging from accepted CI bundles;
- durable activation preparation for the immutable launcher.

No market/account/risk/order business logic belongs here. Normal controller updates never overwrite the running controller and never mutate the Scheduler definition. A migration-only legacy activator exists solely to cross the pre-launcher bootstrap boundary and is fail-closed once launcher-root exists.

Physical rollback acceptance uses one bounded typed hook, `AcceptanceFailNextControllerActivation`: it can only arm the already-staged next controller candidate to exit before its READY proof. The marker is content-bound to that candidate, one-shot, lives under the launcher activation root, and is cleared by abort/commit/rollback. It is not a generic crash, shell, path or command surface.

Remote legacy `BuildAgent` is not a production execution path and fails closed. Normal production deployment accepts only a verified hosted-CI artifact whose provenance matches current accepted source tree and binary hashes.

## ChatGPT primary, fallback DATA and CONTROL planes

### Primary Cloudflare MCP

Normal ChatGPT product queries use:

```text
ChatGPT
 -> OAuth MCP Worker
 -> RuntimeSession Durable Object
 -> existing authenticated outbound Windows WSS
 -> okx-agent typed AgentRequest
 -> existing observation / analysis / execution-status owners
 -> bounded typed AgentResponse
 -> ChatGPT
```

Acceptance rule: a new product capability is not T4-accepted until that capability itself is callable through the connected `okx-cloudflare-mcp` surface. A fresh Worker version or healthy transport status alone is insufficient. If ChatGPT has a stale tool schema, the tools must be refreshed and the primary call repeated.

Cloudflare owns transport/auth/correlation only. Product calculations and exchange truth remain Windows/Rust-owned.

### Fallback DATA #10

GitHub encrypted DATA remains an independent fallback/parity/recovery path, not the normal product path:

```text
client
 -> X25519/HKDF/ChaCha20-Poly1305 envelope
 -> GitHub issue #10
 -> okx-agent
 -> same typed operation / same product owners
 -> encrypted terminal response
```

It keeps request correlation, replay suppression, bounded cursor recovery and restart/network recovery. GitHub issue history is transport evidence, never market/account state authority. Fallback success cannot substitute for missing primary Cloudflare capability acceptance.

### CONTROL #12

Typed Windows operations only:

```text
client
 -> okx.windows.control/v1
 -> GitHub issue #12
 -> okx-host-control
 -> fixed allowlisted lifecycle/deploy/diagnostic operation
 -> okx.windows.control.result/v1
```

No arbitrary shell, PowerShell, path, script or HTTP proxy exists.

## Windows supervision

```text
Task Scheduler
  ONE TimeTrigger
  PT1M indefinite
  StartWhenAvailable=true
  IgnoreNew
        |
        v
ONE okx-host-launcher
  one-shot / no polling
  immutable active pointer
  transactional activation + rollback
        |
        v
ONE okx-host-control
  mutex
  desired state
  Job Object / KILL_ON_JOB_CLOSE
  bounded child restart 1/5/15/30/60
        |
        v
ONE okx-agent
```

Task Scheduler supervises the launcher only. The launcher selects/starts exactly one immutable controller version; host-control supervises the agent only. During the one-time root migration, the accepted controller only materializes the verified launcher root and a durable migration record, then publishes terminal CONTROL PASS. A bounded migration-only child waits for the exact parent process identity to exit, performs the single legacy-to-launcher Scheduler action replacement, verifies the resulting launcher policy, and invokes the existing task. If that child fails, the unchanged legacy task starts the same accepted controller, which recovers the migration from durable GitHub terminal evidence and retries. Normal subsequent controller updates never mutate Scheduler.

No SCM service, RestartOnFailure authority, LogonTrigger recovery, PowerShell watchdog or second custom supervisor is allowed.

Real reboot/sign-in and real external GitHub/network-loss recovery are physically accepted.

## Filesystem trust boundaries

```text
C:\okx
  mutable canonical Git workspace

C:\okx-control
  immutable okx-host-launcher.exe
  content-addressed controller versions
  active/staged/activation lifecycle state

C:\okx-runtime
  installed okx-agent.exe
  runtime state/logs/staging

C:\okx-upgrade
  temporary bootstrap/upgrade staging only
```

Installed production binaries do not execute from the mutable checkout.

## Supply-chain authority

The production build/deploy path is:

```text
committed source + Cargo.lock
 -> pinned GitHub Actions
 -> Rust 1.95.0
 -> cargo --locked
 -> PR CI
 -> exact tested source tree
 -> merge
 -> merged tree must equal tested PR head tree for CI reuse
 -> bundle manifest + binary SHA-256
 -> verified deploy
```

Durable recovery promotion additionally verifies the exact tested/merged tree, embedded manifest and binary hashes before publishing a versioned GitHub Release plus `SHA256SUMS.txt`.

A short-lived Actions artifact is therefore no longer the only disaster-recovery source.

`main` is protected with the canonical required CI checks. Artifact acceptance still requires the exact tested PR head tree to equal the merged `main` tree.

## Account boundary

Production trading authority is isolated in the standard OKX sub-account `Succession`.

```text
OKX main account
  treasury / administrative authority
  no runtime trading API
        |
        v
Succession standard sub-account
  Futures mode
  long/short position mode
        |
        +-- observer key: Read only
        |
        +-- executor key: Read + Trade, never Withdraw
```

Verified account invariants:

- Global production endpoint family;
- standard sub-account identity;
- Futures account mode;
- `long_short_mode`;
- observer has read-only and neither trade nor withdraw;
- executor has read + trade and never withdraw;
- observer/executor fingerprints target the same intended account;
- executor IP binding may be present or absent and is diagnostic only;
- raw API key, secret, passphrase and raw UID are never returned through DATA/CONTROL/logs.

GitHub Environment variables are not a live-trading enable authority. Runtime correctness depends on the production executor boundary, whose accepted constructor remains disabled.

## Observation and query model

Level 1 forensic/detail operations expose bounded factual evidence such as:

- InstrumentRules;
- FindInstruments;
- MarketSnapshot / MarketOverview / MarketHistory;
- SnapshotQuality;
- AccountSnapshot;
- transport telemetry;
- ExecutionStatus.

Level 2 bounded application/research operations compose immutable dependencies locally:

- MarketResearch;
- PortfolioRisk;
- CurrentCost;
- PositionScenario;
- AnalyzeCandidateOrder.

Admission rule:

> deterministic arithmetic, aggregation, filtering, consistency checks and bounded scenario expansion belong locally beside immutable inputs rather than in ChatGPT over large raw responses.

Level 2 creates no new engine or state owner.

### Universal bounded analytical plan

The system must answer broad classes of questions without adding one operation per natural-language phrasing. ChatGPT may translate a question into one **versioned bounded typed read plan** composed from allowlisted enums, while Rust remains the factual and numerical authority.

A read plan may contain only:
- a bounded universe selector over normalized facts (for example derivative type, settlement currency and instrument state);
- an allowlisted field projection;
- typed predicates over those fields;
- stable sort + top/bottom-K;
- bounded grouping/aggregation;
- explicitly supported time-window comparisons;
- versioned deterministic metrics from `okx-analysis`;
- an explicit freshness/coherence requirement and hard output budget.

The evaluator is not a new state owner. It runs over existing immutable snapshots / bounded one-shot reads and delegates formulas to `okx-analysis`.

Preferred stable MCP read surface:
- `query_capabilities`: returns a versioned catalog of supported field/metric/operator IDs, units, required evidence classes and hard limits;
- `query`: accepts a bounded plan plus the exact catalog version used to construct it.

The runtime maps external string IDs to internal typed enums/metric implementations and rejects unknown or stale IDs. This allows the metric catalog to evolve without creating a new MCP tool per metric or question while preserving deterministic validation.

This is deliberately **not** generic SQL, JavaScript, a string expression evaluator, arbitrary endpoint composition or user-provided executable code. Unsupported fields/operators/metrics fail closed at validation. Mutation, risk-policy enforcement and execution never pass through this generic read plan.

Consequences:
- “top 10 gainers”, “bottom 5 by 24h change”, “highest volume USDT swaps” and similar questions become different plans over the same capability, not different backend endpoints;
- backend code changes only when a genuinely new factual primitive or metric is required;
- the MCP surface stays small and stable while question coverage grows through safe composition;
- large universe work is performed below ChatGPT and only compact evidence crosses MCP.

## Market/order-state integrity

Persistent WebSocket state is the live owner after REST bootstrap/recovery.

A connected socket alone is not readiness. FRESH requires accepted connection/subscription generations and coherent source updates.

For OKX order-book channels, `seqId/prevSeqId` continuity is authoritative. Deprecated checksum fields must not be treated as integrity authority.

## Execution safety

```text
immutable accepted inputs
      |
      v
ExecutionIntent
      |
      v
validated ExecutionPlan
      |
      v
ONE OrderExecutor
      |
      +--> durable PREPARED
      |
      +--> future live path only:
             reacquire current authoritative state
             exact continuity/risk checks
             persist SUBMITTING
             send once
             ACK or UNKNOWN_SUBMISSION
             reconcile exact exchange evidence
```

Rules:

- identical intent + identical plan is idempotent;
- intent/plan conflict is terminal REJECTED;
- client-order-id collision is terminal REJECTED;
- capacity exhaustion is terminal FAILED;
- corruption/I/O/invariant failures fail closed;
- network uncertainty after a send never causes blind retry;
- UNKNOWN_SUBMISSION is reconciled by exact client-order/exchange evidence;
- observation and analysis never send orders.

Phase 2 pre-enable is physically accepted:

- executor credential preflight PASS;
- disabled submit returns `LIVE_TRADING_DISABLED`;
- gate occurs before SUBMITTING and before exchange send;
- ledger survives restart unchanged;
- live orders sent in acceptance = 0.

Any future live-write work is a separate explicitly authorized post-#113 acceptance slice.

## Current acceptance state

Canonical forward authority is issue #160 / `docs/ROADMAP.md`.

Stage 1 status:
- Repository Guard v1: ACCEPTED;
- P0.1 WebSocket 443: ACCEPTED;
- P0.2 exchange clock discipline: ACCEPTED;
- venue/instrument-state execution gate: ACCEPTED;
- account + ledger truth: ACCEPTED;
- P0.3 named rate/backpressure: ACCEPTED;
- Stage-1 final T1–T5 acceptance: ACCEPTED/CLOSED;
- exact final #173 artifact is installed with verified provenance;
- Cloudflare-primary and encrypted GitHub fallback parity passed on the exact binary;
- exact-tree restart/recovery reconverged account and public market evidence to FRESH;
- current forward cursor is Stage 2 — INTELLIGENCE + RISK.

Primary Cloudflare MCP is live and the refreshed ChatGPT tool surface can call `account_summary` under tool contract `okx.mcp.tools/2026-10-01.2`; GitHub DATA remains fallback/parity only.

## Non-goals

- no withdrawal/transfer API;
- no arbitrary shell/HTTP proxy;
- no arbitrary SQL/string-expression/executable query language; only the bounded typed analytical plan described above;
- no local LLM/SQL state layer;
- no autonomous strategy engine before the roadmap creates a real strategy boundary;
- no additional access transport beyond the accepted Cloudflare-primary + GitHub-fallback/control topology without a reproduced need;
- no second lifecycle supervisor;
- no live order mutation until separately authorized and accepted.

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
        +----------------------+----------------------+--------------------+
        |                      |                      |                    |
        v                      v                      v                    v
 Observation             Pure Analysis        Research boundary     ONE OrderExecutor
 Reference/Market        Decimal math          replay/lineage        durable ledger
 Account/Orders          features/risk         immutable artifacts   mutation authority
 reconciliation          strategy formulas    no exchange owner      hard-disabled prod
 readiness              no state owner        no second runtime
        |                      |                      ^
        +----------------------+----------------------+
                               |
                               v
                    immutable normalized/research evidence

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

Inputs are immutable accepted snapshots. Portfolio mandate/hard-policy arithmetic, covariance/correlation, scenario/stress and future statistical risk calculations belong here as pure deterministic/statistical functions. Stage-3C validation statistics (for example applicable Sharpe-like/DSR/PBO primitives) and strategy research metadata such as lookback/forward holding horizon also belong here as pure versioned functions; promotion/family/holdout state does not. No collector, transport, lifecycle or mutation ownership lives here.

### `okx-research` (Stage-3 admitted boundary)

One new research-domain crate is explicitly admitted for Stage 3 if its implementation matches #160.

It owns the Stage-3 research boundary and may own:
- immutable dataset/archive manifests and content hashes;
- point-in-time/availability-time research views;
- deterministic replay orchestration over normalized historical facts;
- hypothesis / experiment / validation lineage;
- versioned baseline experiment/profile construction for replay (assumptions, declared cost model, mandate and hard-policy values), while policy arithmetic remains in `okx-analysis`;
- walk-forward/OOS/anti-overfit orchestration over immutable parent/slice artifacts;
- immutable research-family, frozen validation-spec, holdout-consumption and promotion lineage;
- immutable promotion bundles;
- compact paper/shadow research evidence.

It must consume normalized facts and deterministic `okx-analysis` functions. It does **not** become a second formula/risk owner.

It must not own:
- OKX credentials or raw authenticated exchange authority;
- HTTP/WebSocket connectivity;
- current live Reference/Market/Account state;
- a second Tokio runtime;
- a daemon/service/watchdog;
- a scheduler or polling loop;
- SQL/general query execution;
- an operational mutable status database;
- exchange order mutation;
- Cloudflare/GitHub transport.

The research artifact repository is content-addressed/immutable evidence, not live state. The same artifact bytes/config map to the same identity; accepted artifacts are never silently rewritten.

Replay provenance distinguishes two source trees: `dataset_source_tree` identifies the build that captured/normalized the immutable dataset, while `replay_source_tree` identifies the exact build whose strategy/risk/replay code produced the experiment. `replay_source_tree` is part of `ExperimentSpec` identity; changing replay code therefore changes experiment identity even when the dataset is unchanged.

Stage 3C must reuse this replay owner rather than implement a validation-specific simulator. A validation split is represented as immutable, content-addressed slice evidence derived from a parent research dataset and bound by a frozen validation specification. Long-range acquisition reuses the existing cursor-capable historical adapters plus immutable checkpoint lineage; it is not a second collector or background job system. Research-family manifests enumerate existing trial artifacts and do not authorize an autonomous parameter-search service.

A separate `okx-strategy` crate is not admitted for Stage 3 v1. Production-intended feature/strategy logic belongs as deterministic modules under `okx-analysis` until a reproduced ownership/dependency problem justifies another boundary.

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

It does not define research strategy/risk/cost policy defaults. For replay it validates the bounded request, loads immutable artifacts, invokes `okx-research`, and returns the compact result. It does not duplicate observation/runtime/execution state machines.

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

Normal ChatGPT-facing analytical/research responses are **decision packets**, not data exports. The normal target is one coarse capability call producing <= 12,288 bytes with identity/provenance, quality/status, assumptions, diagnostics, risk/policy outcome, compact numerical evidence, invalidation/blocker information and artifact/continuation ids where relevant. Raw candles, trades, L2 events, ledger rows and replay event traces remain local forensic/artifact evidence and are not returned merely so ChatGPT can recompute Rust-owned math.

A normal user question should usually require one coarse MCP operation after capability discovery is already known. Additional round-trips are evidence-driven (for example `NOT_READY`, `DEGRADED`, explicit forensic inspection or continuation), not a fixed chain of low-level reads.

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

Stage-1 account truth is scoped to this authenticated production execution account. A separate Main-account Read-only credential has now been provisioned and independently authenticated, but it is **not connected to the production runtime** and does not alter the Succession observer/executor boundary. Full main+subaccounts treasury aggregation remains deferred until a roadmap stage actually requires that capability and the existing architecture is extended deliberately; until then `multi_account_inventory_complete=false` remains expected. The Main read credential must not be introduced merely to force an unsupported exchange oracle.

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
             revalidate current reference/venue/account generations
             re-evaluate the accepted versioned hard-risk policy
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
- observation and analysis never send orders;
- the exact mandate/hard-policy used for mutation admission is immutable plan evidence, not a submit-time caller override;
- risk-increasing OPEN requires the current analysis to match the plan binding, current account generation, immutable candidate notional/loss and current exact configured leverage;
- a validated CLOSE may remain risk-reducing even when the account is already beyond an open-risk policy threshold; ordinary close-capacity, account/reference/venue/clock checks still apply.

Phase 2 pre-enable is physically accepted:

- executor credential preflight PASS;
- disabled submit returns `LIVE_TRADING_DISABLED`;
- gate occurs before SUBMITTING and before exchange send;
- ledger survives restart unchanged;
- live orders sent in acceptance = 0.

Any future production live-write work remains behind the final explicit activation gate after Stages 1–5; current Stage-2 work may extend pre-mutation validation while production writes remain disabled.

## Current acceptance state

Canonical forward authority is issue #160 / `docs/ROADMAP.md`.

Stage 1 status:
- Repository Guard v1: ACCEPTED;
- P0.1 WebSocket 443: ACCEPTED;
- P0.2 exchange clock discipline: ACCEPTED;
- venue/instrument-state execution gate: ACCEPTED;
- account + ledger truth for the authenticated production execution account: ACCEPTED;
- P0.3 named rate/backpressure: ACCEPTED;
- Stage-1 final T1–T5 acceptance: ACCEPTED/CLOSED for that scope;
- a separate Main-account Read-only credential exists and authenticates but is intentionally not production-connected; treasury-wide aggregation remains deferred until deliberately integrated and accepted;
- exact final #173 artifact is installed with verified provenance;
- Cloudflare-primary and encrypted GitHub fallback parity passed on the exact binary;
- exact-tree restart/recovery reconverged account and public market evidence to FRESH.

Stage 2 status: **ACCEPTED/CLOSED**.
- bounded universal analytical query: ACCEPTED;
- live microstructure core: ACCEPTED;
- derivatives/history intelligence: ACCEPTED;
- portfolio mandate/hard-policy primary T4: ACCEPTED through connected Cloudflare MCP with permissive FRESH/coherent evidence and intentional typed policy rejection;
- portfolio-risk correctness closure: ACCEPTED;
- statistical portfolio risk: ACCEPTED;
- pre-mutation hard-policy ownership: ACCEPTED inside the existing execution authority;
- account-mode-aware virtual risk: ACCEPTED in PR #197. Authenticated Futures mode (`acctLv=2`) uses deterministic local COUNTERFACTUAL portfolio risk over FRESH OKX mark/reference facts and exchange-published contract/lot/min/max/leverage constraints. The real observed account remains independently checked against `account-position-risk`;
- Position Builder remains a typed independent virtual-portfolio oracle only for `acctLv=3/4`; it is no longer a mandatory gate for Futures mode;
- final exact tested/deployed tree: `b63f98d57df6e9e1e1b6fb1c24c949d580717603`; tested head `efa6f8dec36c3abde0d9abc69f749d4b75c0b05c`; merged main `f1028e97f984e313f9c0b702010ebeca4518a088`;
- CI #843 / run `37160080767`: all jobs PASS; Windows artifact `11287008643` deployed with persisted source/hash provenance; installed agent SHA-256 `1bdba04deb1f9a93761f7521279341d32dfaca883046df49da378aa9ac2b02d6`;
- primary Cloudflare `portfolio_risk/v6` representative non-zero Futures proof: PASS/FRESH; BTC+ETH gross notional `11.16057 USD`, exact initial margin `2.232114 USD`, statistical evidence READY, current observed-account oracle consistent;
- negative proof: hard-policy `MAX_LOSS_PER_TRADE` rejection PASS and Futures exchange-constraint leverage rejection PASS;
- post-proof account remains FRESH/coherent, zero positions/pending orders, consistent reconciliation; exchange mutations = 0;
- structural corrective delta: zero new crates/tasks/state owners/stores/schedulers/transports/mutation authorities/MCP tools/credential models/dependencies;
- CONTROL HTTP reliability hardening remains ACCEPTED (#193);
- production live trading remains disabled;
- current roadmap cursor: **Stage 3 — SCIENTIFIC RESEARCH + REPLAY**.

Primary Cloudflare MCP is healthy on Worker contract `okx.mcp.tools/2026-10-05.9`; the connected runtime is PASS/connected/session_fresh and research catalog `okx.research.catalog/2026-10-05.8` is callable. Runtime connection generation is operational telemetry and is intentionally not a documentation authority. Account truth remains FRESH/coherent with zero open positions/pending orders and consistent ledger reconciliation. GitHub DATA remains fallback/parity only.

Stage 3A/3B/3C infrastructure is accepted. H1 `close_momentum` was scientifically REJECTED. H2 `two_bar_momentum` passed the frozen development screen and is waiting for its fresh independent 96h FINAL_OOS in #216. The Stage-3D immutable promotion gate is accepted on main; the active implementation cursor is the causal live-decision parity kernel before bounded PAPER/SHADOW session plumbing.

## Stage-3 research ownership and product boundary

The canonical Stage-3 contract is issue #160. This section records the architectural constraints that must remain true while implementing it.

### Historical truth

The existing Stage-2 bounded history adapters are **recent-analysis inputs**, not a complete replay store. Stage-3 replay must not infer historical eligibility/reference parameters from only the current `ReferenceRegistry`.

Historical research evidence must carry:
- point-in-time universe membership/listing/expiry/state;
- contract/reference parameters applicable at that time;
- event time and availability time;
- source/chunk/parser/normalization identity;
- explicit gaps, duplicates and out-of-order evidence;
- Tier-A or Tier-B capability classification.

The current `ReferenceRegistry` remains the only owner of **current** reference truth. Historical research manifests are immutable evidence, not a competing live registry.

Official OKX historical data/query sources are preferred where available, but external availability never substitutes for local manifest/hash/gap validation.

### Replay tiers

```text
Tier A
candles + funding + point-in-time reference
+ explicit fee/spread/slippage model
=> MODELLED_EXECUTION

Tier B
provenance-locked trades/order-book/reference events
+ event/availability ordering
=> event replay
```

A Tier-A result must never be described as historical L2 execution. Missing required Tier-B data is `INSUFFICIENT_DATA`.

### Research persistence

Permitted persistence is a local content-addressed immutable artifact repository for:
- datasets/chunks;
- experiment specs/results;
- promotion bundles;
- paper/shadow evidence.

It must use atomic verified publication and hash-addressed identity. It is not an operational database and cannot become authority for live market/account/execution state.

Negative experiments remain part of lineage. A later version cannot rewrite an earlier experiment or promotion decision.

### Stage-3 causal/replay invariants

Research replay is causal, not merely timestamp-sorted.

Required temporal roles:
- `event_time`: when the exchange event/fact occurred;
- `available_time`: earliest time the strategy may use the fact;
- `decision_time`: when the deterministic strategy produced its decision;
- `earliest_execution_time`: earliest admissible simulated/real execution time.

A completed-bar signal cannot be filled at the already-known same close unless the admitted replay source explicitly proves that execution semantics.

Tier-A bar replay never guesses favorable hidden intrabar order. If stop/target/limit/liquidation ordering is unknowable from admitted data, use a declared conservative deterministic rule, return `AMBIGUOUS_FILL`, or require Tier B.

Price roles are typed and distinct: signal, mark, index and execution. Leverage/margin/liquidation claims must use the role required by the venue model; missing required history is explicit `INSUFFICIENT_DATA` or a declared conservative counterfactual, never silent substitution.

Funding is replayed as dated events using admitted interval/mechanism evidence. Fees, spread/slippage/impact and capacity/sizing assumptions are versioned and carry provenance; present-day fees are never silently called historical.

### Stage-3 selection/holdout invariants

All research variants belong to immutable family lineage:
- human-authored;
- ChatGPT-authored;
- parameter sweeps;
- failed/negative trials.

The family records parent/trial identity, changed fields and declared search/selection context where applicable. Promotion evidence cannot hide losing variants.

Final OOS is sealed before candidate freeze. Opening it creates immutable consumption evidence. A descendant changed after viewing the holdout must label that interval `POST_SELECTION`, not pristine OOS.

Evidence adequacy is first-class. Too few/effectively dependent observations may return `INSUFFICIENT_EVIDENCE` even when point estimates look attractive.

Before alpha claims, replay accounting must pass:
- a `NO_TRADE` null baseline with zero trades/fees/funding/PnL;
- one simple manually auditable deterministic baseline.

### Research artifact/storage invariants

Artifact identity binds both source and normalized evidence:
- source identity/request/range/acquisition metadata;
- raw content SHA-256 + size;
- parser/normalization/schema versions;
- normalized canonical content SHA-256 + row/event count;
- deterministic field/map ordering, decimal formatting and UTC time encoding.

A stable URL never implies stable bytes.

Storage has two roles:
- `EvidenceStore`: pinned accepted manifests/results/promotion evidence and required reproducibility chunks; no silent eviction;
- `SourceCache`: large re-downloadable raw chunks; bounded quota and evictable only when unpinned.

Disk quota and free-space floor are explicit. Exhaustion fails with typed `STORAGE_BUDGET_EXCEEDED` instead of threatening the runtime.

Historical archives are untrusted external input:
- allowlisted official source families/hosts only;
- no arbitrary URL fetch;
- bounded compressed/decompressed bytes, rows/events and decompression ratio;
- no archive path traversal;
- strict numeric/time/reference validation;
- malformed/corrupt input fails closed.

### Research control vs live decision path

Cloudflare/ChatGPT is a research control/inspection plane, never a live decision latency dependency.

```text
user -> ChatGPT -> MCP -> start / inspect / resume / authorize promotion

OKX live feed
 -> existing runtime/observation
 -> local versioned strategy
 -> Stage-2 risk
 -> PAPER simulated execution
    or SHADOW WOULD_SUBMIT
 -> immutable research evidence
```

Two independent freshness domains are reported:
- `MCP_TRANSPORT`: whether new remote commands can be safely accepted;
- `RESEARCH_INPUT`: whether the underlying market/reference/account facts are fresh/complete enough for a local research decision.

A stale MCP session does not by itself terminate an already accepted local paper/shadow run. Stale/gapped research input blocks the affected local decision and records a typed reject.

### PAPER / SHADOW semantics and authority

`PAPER`:
- consumes live accepted facts;
- uses research simulated execution;
- records virtual positions/PnL;
- performs no exchange mutation.

`SHADOW`:
- runs the production-intended signal/candidate/Stage-2-risk/pre-execution path as far as practical;
- records `WOULD_SUBMIT`;
- stops before any exchange order mutation.

Stage 4 Demo Trading is the first stage permitted to prove actual order mutation.

The research engine may automatically classify `REJECT`, `INSUFFICIENT_DATA` or `INSUFFICIENT_EVIDENCE`. Positive promotions `BACKTESTED -> PAPER -> SHADOW` require explicit operator authorization recorded in immutable lineage. Stage 3 can never authorize live trading.

### Scientific boundary

Minimum accepted methodology:
- chronological train/validation/final OOS;
- walk-forward;
- explicit final-holdout consumption;
- purge/embargo only when overlapping outcome/holding windows require it;
- versioned fee/funding/spread/slippage/capacity assumptions;
- cost and parameter sensitivity;
- regime diagnostics;
- negative-trial retention;
- DSR/PBO/CSCV only where their declared assumptions apply.

Unsupported diagnostics are explicit `NOT_APPLICABLE`, not fabricated.

### ChatGPT research UX

ChatGPT is the research strategist and evidence consumer, not the bulk-history compute engine.

```text
user objective
 -> ChatGPT typed hypothesis/experiment design
 -> Windows Rust data/replay/statistics/risk
 -> immutable research artifacts
 -> compact MCP evidence
 -> ChatGPT critique/interpretation/next version
```

Raw bulk candles/trades/L2 do not cross the chat transport for calculation.

The Stage-3 MCP surface should stay at most:
- `research_capabilities`;
- `research`.

New strategy variants are data/contracts, not new MCP methods.

A normal research result targets <= 12,288 bytes. Detailed evidence is inspected by bounded artifact/experiment id.

### Transport and long-running research

Stage 3 preserves the accepted direct-transport limits instead of raising them:
- 64 KiB direct frame limit;
- 20-second response deadline;
- existing bounded direct in-flight request count.

Large acquisition/replay is expressed as deterministic bounded steps:
- one call performs a declared bounded work budget (source requests/input bytes/events) within the existing deadline;
- a nonterminal result returns an immutable parent-linked checkpoint/continuation identity plus remaining cursor;
- the next call resumes from that identity with no mutable job-status database;
- ChatGPT may execute several bounded calls synchronously within one user turn; users are not expected to manually advance every source chunk;
- completed chunks/checkpoints are idempotent/content-addressed;
- response loss can be retried without duplicating exchange actions or rewriting evidence.

This is resumable request/response work, **not** a background daemon, job queue, polling scheduler or second runtime.

A new chat must be able to inspect/resume an experiment by immutable id. Conversation memory is convenience only, never research state.

### Paper/shadow

Stage-3 paper/shadow consumes the accepted existing live observation path. It may add one bounded event-driven research-session component inside the existing agent/runtime ownership chain if required by the accepted slice.

It must not:
- create another market WebSocket owner;
- permanently subscribe to the whole market;
- add a timer/polling scheduler;
- place/amend/cancel exchange orders;
- bypass Stage-2 risk.

Replay and live shadow must call the same versioned `okx-analysis` feature/strategy/risk logic wherever semantics are intended to match.

Promotion sequence before Stage 4:

```text
RESEARCH -> BACKTESTED -> PAPER -> SHADOW
```

No Stage-3 state grants exchange mutation authority.

### Stage-3A v1 implementation scope

The first implementation is deliberately narrow so the research truth layer can be physically proven before broadening the universe:

- USDT linear perpetuals only;
- BTC-USDT-SWAP, ETH-USDT-SWAP and DOGE-USDT-SWAP as initial Tier-A instruments;
- 1H Tier-A cadence;
- candles + funding + point-in-time instrument/reference timeline;
- historical mark/index evidence only when required by the admitted strategy/risk claim;
- one bounded BTC-USDT-SWAP Tier-B historical-trades event sample for **schema/provenance/event-continuity semantics proof**, not a generic L2 engine. Stage 3A v1 uses the existing bounded OKX public REST owner and deliberately does not add an archive downloader.

Stage 3A v1 explicitly does not attempt all OKX products, all cadences or full L2 history.

If historical reference facts cannot be proven for a requested interval, return `INSUFFICIENT_REFERENCE_HISTORY`; never reconstruct them from the current registry by assumption.

### Stage-3 architecture budget

Expected maximum structural delta for the whole stage:

```text
new Rust crates                <= 1 (okx-research)
new Tokio runtimes              0
new daemons/services            0
new schedulers/poll loops       0
new operational databases       0
new live fact owners            0
new exchange mutation owners    0
new transports                  0
new MCP tools                  <= 2
new strategy-formula owners     0
production exchange mutations   0
```

Any larger delta requires a reproduced product/ownership failure and explicit architecture review before code is merged.

### Stage-3 design freeze

The accepted #160/ROADMAP contract plus these invariants is the final pre-implementation architecture pass.

Stage 3A v1 is **PASS/CLOSED** on accepted source tree `5d9a73e385022e10792d9fa75b1c5b70f6aecdb6`.

Accepted Stage-3A runtime shape:
- exactly one new pure `okx-research` crate;
- no new Tokio runtime, daemon, scheduler/poller, operational database, transport, live fact owner or exchange mutation authority;
- historical acquisition remains in the existing OKX public REST/rate-budget owner;
- `capture_id` binds exact raw response bytes while `chunk_id` binds canonical normalized evidence, so transport-envelope timing noise does not destroy normalized idempotency;
- immutable content-addressed evidence/cache/checkpoint boundaries persist locally; bulk historical rows do not cross MCP;
- Tier-A current-reference evidence is explicitly current-only and produces `INSUFFICIENT_REFERENCE_HISTORY` when historical point-in-time coverage cannot be proven;
- the bounded Tier-B v1 proof uses BTC historical trade events through the existing public REST owner rather than introducing a separate archive acquisition subsystem;
- the external research surface remains `research_capabilities` + `research`.

The next cursor is **Stage 3B — Deterministic Replay Kernel**. Do not add framework layers, services, statistical methods or storage authorities for speculative completeness. Any non-planned structural delta requires a reproduced source/runtime/test contradiction and explicit architecture review.

## Non-goals

- no withdrawal/transfer API;
- no arbitrary shell/HTTP proxy;
- no arbitrary SQL/string-expression/executable query language; only the bounded typed analytical plan described above;
- no local LLM/SQL state layer;
- no autonomous/self-modifying strategy engine; Stage-3 strategies are deterministic versioned functions plus immutable experiment/promotion evidence;
- no additional access transport beyond the accepted Cloudflare-primary + GitHub-fallback/control topology without a reproduced need;
- no second lifecycle supervisor;
- no live order mutation until separately authorized and accepted.

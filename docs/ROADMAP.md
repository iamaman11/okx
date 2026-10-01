# Roadmap and acceptance cursor

## Single canonical plan

**#160 — Industrial trading platform roadmap**

Issue #160 is the authoritative detailed roadmap and acceptance contract. This file is the repository-side summary/cursor only; it must not become a second competing plan.

The platform follows exactly five development stages:

```text
Repository Guard v1
    ->
Stage 1  TRUTH
    ->
Stage 2  INTELLIGENCE + RISK
    ->
Stage 3  SCIENTIFIC RESEARCH + REPLAY
    ->
Stage 4  EXECUTION + TCA
    ->
Stage 5  GOVERNANCE + OPERATIONS + LEARNING
    ->
explicit optional live activation gate
```

The final live activation gate is not a sixth development stage. Production live trading remains fail-closed until separately authorized after Stages 1–5 pass.

## Current Stage-1 cursor

Canonical execution order inside Stage 1:

1. **P0.1 WebSocket 443 compatibility** — ACCEPTED.
2. **P0.2 exchange clock discipline** — ACCEPTED.
3. **Venue/instrument-state execution gate** — ACCEPTED.
4. **Account + ledger truth** — ACCEPTED.
5. **P0.3 named rate/backpressure domains** — CURRENT.
6. **Stage-1 final T1–T5 acceptance**.

The P0/P1 labels are capability groups, not a competing execution order.

Current P0.3 named rate/backpressure slice must prove:
- rate/backpressure is modeled by the named OKX domains that actually exist, not one global requests-per-second counter;
- public REST/IP, private REST/User ID, WS connection/login/subscription, order-management, instrument/family and sub-account aggregate scopes remain distinguishable where OKX defines them;
- typed throttle evidence carries exchange code/domain (including 50011 and 50061 where applicable), operation class, relevant account/instrument/family scope, attempt count and the bounded local defer/backoff decision;
- the runtime never invents a server Retry-After value when OKX does not provide one;
- read-only account-rate-limit/fill-ratio evidence is ingested as current exchange evidence where the credential/tier exposes it;
- no tight retry loop or blind mutation retry is introduced; uncertain-result/idempotency rules remain authoritative;
- expensive research/history work yields to heartbeat/control and mutation reconciliation, while existing bounded concurrency/response-size limits are preserved;
- ownership stays inside the existing runtime/API boundary: no generic limiter service, second scheduler, daemon or new state authority.

## Repository Guard v1

The guard exists to preserve the accepted architecture while keeping development fast.

Machine-enforced:
- protected `main` keeps the required contexts;
- CI validates architecture dependency direction, portable Rust and Cloudflare MCP;
- `Cargo.lock` + `--locked` remain mandatory;
- the Cloudflare package graph is locked and CI/deploy uses the committed lock;
- exact tested source tree and Windows artifact SHA-256 provenance remain mandatory.

Review-enforced:
- every nontrivial change names one concrete need/failure;
- an existing authoritative owner is preferred;
- structural complexity delta defaults to zero;
- every non-zero crate/task/store/transport/dependency/tool delta is justified;
- superseded paths are removed rather than retained indefinitely;
- tests name the product/architecture failure they protect.

The executable dependency DAG lives in `scripts/check_architecture.py`. The current ownership model remains canonical in `docs/ARCHITECTURE.md`.

## Cross-stage evidence rules

Professional outputs are decision-grade evidence, not raw API dumps.

Required principles:
- multi-source facts carry explicit `as_of`, provenance/generation and coherence/skew diagnostics;
- older/out-of-order REST evidence cannot regress newer accepted state;
- evidence distinguishes **OBSERVED**, **MODELLED** and **COUNTERFACTUAL** values;
- replay uses point-in-time and availability-time semantics;
- analytical outputs carry assumptions, uncertainty/sensitivity where meaningful, quality and invalidation conditions;
- exchange risk/max-size/precheck surfaces may validate local calculations but never replace deterministic local ownership;
- manual/external/exchange-system actions remain separately attributed unless managed lineage exists;
- one logical mutation identity is transport-independent across Cloudflare primary and GitHub fallback.

## Five development stages

### Stage 1 — TRUTH

Compatibility, clock/rate correctness, venue/reference safety, coherent account hierarchy, balances, positions, orders, fills, bills, PnL/funding/fees/interest and reconciliation.

Important proof includes:
- WS 443 and reconnect;
- exchange clock gate;
- venue/instrument/reference pre-mutation gate;
- account/ledger history coverage and no double counting;
- named rate domains/backpressure;
- direct Cloudflare MCP plus independent GitHub fallback.

Exit: factual market/account/reference truth is coherent, fresh/provenanced and black-box accepted.

### Stage 2 — INTELLIGENCE + RISK

Market microstructure, derivatives structure, portfolio exposures, stress/scenario evidence, a versioned trading/research mandate and deterministic hard risk policy.

Important proof includes:
- decision-grade evidence with explicit coherence;
- depth/spread/impact/basis/carry;
- exact sizing/margin/risk boundaries;
- exchange-oracle differential checks against supported account-position-risk / max-size / position-builder evidence;
- no duplicate venue/reference owner.

Exit: professional decision evidence is calculated locally in Rust and ChatGPT does not become the financial calculator or safety authority.

### Stage 3 — SCIENTIFIC RESEARCH + REPLAY

Falsifiable hypotheses, immutable experiment lineage, point-in-time datasets, availability-time-safe replay, OOS/walk-forward research, cost-aware simulation and paper/shadow lifecycle.

Important proof includes:
- immutable dataset/archive manifests and gap evidence;
- no look-ahead/survivorship/reference-data leakage;
- purging/embargo where overlapping horizons require it;
- multiple-testing evidence and negative-trial retention;
- the same deterministic feature/strategy implementation across replay/paper/live where semantics should match;
- immutable promotion bundles with declared promotion and demotion criteria.

Exit: strategies are scientifically testable without live money or hidden future data.

### Stage 4 — EXECUTION + TCA

Complete order/position lifecycle, single mutation authority, uncertain-submit recovery, partial fills, amend/cancel/protective orders, intent arbitration and execution-quality attribution.

Important proof includes:
- idempotent client identity and transport-independent duplicate protection;
- reduce-only/hedge/reverse/STP semantics;
- deterministic arbitration for simultaneous same-instrument managed intents;
- manual/external changes force fresh reconciliation;
- pre-trade exchange/risk/reference revalidation;
- Demo Trading place/observe/amend/cancel/close;
- TCA with fills/bills, latency and modelled-vs-realized cost residuals.

Exit: mutation mechanics are physically accepted in OKX Demo Trading while production live trading remains disabled.

### Stage 5 — GOVERNANCE + OPERATIONS + LEARNING

Hard policy/kill behavior, attribution/scorecards, closed learning loop, operational budgets, recovery, security, release provenance and compatibility review.

The learning loop is:

```text
falsifiable hypothesis
 -> reproducible experiment
 -> promotion bundle
 -> paper/shadow observation
 -> controlled execution
 -> reconciled outcome + TCA
 -> attribution / live-vs-research residuals
 -> review
 -> KEEP | DEMOTE | RETIRE | NEW_VERSION
 -> next hypothesis/version
```

Important proof includes:
- no strategy self-promotion or self-modification;
- immutable review evidence;
- manual/external activity excluded from managed-strategy attribution unless lineage exists;
- explicit freshness/coherence/query/recovery/reconciliation budgets;
- durable execution/audit/research-artifact integrity and restore/reconcile path;
- Cloudflare-primary/GitHub-fallback independence;
- exchange-native disconnect protection such as Cancel All After is Demo-proven safe for the chosen protective-order topology or explicitly rejected with accepted alternative safety evidence;
- relevant OKX API compatibility/deprecation changes are reviewed at release/stage acceptance;
- source -> CI -> artifact -> installed binary provenance and supply-chain evidence.

Exit: the platform is industrial-ready with production live trading still disabled, and research/trading forms a closed auditable evidence loop.

## Stage acceptance contract

Every stage requires one bounded `okx.stage-acceptance/v1` record over one traceable accepted source tree.

Required proof layers:
- **T1** deterministic code/formula/parser/property proof;
- **T2** runtime/in-process ownership, generation, coherence and recovery proof;
- **T3** exact deployable artifact/provenance proof when runtime code changes;
- **T4** supported black-box product proof;
- **T5** negative/fault/recovery proof.

Primary/fallback black-box invariant for every new product capability:
- **Cloudflare MCP is the mandatory primary T4 path and must be exercised first.**
- A healthy `runtime_status` or newer Worker tool-contract version alone does not prove the new capability; the capability itself must be callable through the connected `okx-cloudflare-mcp` tool surface.
- **GitHub encrypted DATA mailbox is fallback/parity proof only** and must not substitute for missing/stale Cloudflare tool exposure.
- If the Worker has a newer tool contract but ChatGPT still exposes an older tool list, primary T4 remains OPEN. Do not route around this by accepting a GitHub DATA result as primary proof.
- If ChatGPT cannot refresh the `okx-cloudflare-mcp` tool schema itself, the operator must use **Обновить инструменты / Update tools** for that plugin, then the primary black-box call must be rerun before acceptance continues.

A stage is PASS only when every deliverable maps to explicit evidence and no unresolved internal correctness/safety gap remains. `BLOCKED_EXTERNAL` is allowed only for a specifically named unavailable credential/venue capability/dataset and is not PASS.

## Final live activation gate

Only after Stages 1–5 PASS:
- explicit operator decision;
- separate least-privilege production Trade credential with `Withdraw=false`;
- credential authority/network/IP-binding posture explicitly accepted;
- live master gate explicitly enabled;
- all fresh clock/venue/reference/account/risk/governance gates accepted;
- tiny bounded canary on one allowed instrument/strategy version;
- successful open -> observe/reconcile -> close -> fills/bills/TCA/attribution cycle;
- immediate disable-new-risk path.

The canary proves production plumbing/safety, not statistical profitability. It cannot auto-promote a strategy or auto-increase capital. Any capital ramp remains a governed learning-loop decision.

No CI or test may silently cross the live gate.

## Accepted foundation retained

Do not reopen without concrete evidence:
- one Tokio runtime / one native agent owner chain;
- reference/public/private observation;
- deterministic analysis boundary;
- durable execution ledger and fail-closed pre-enable boundary;
- verified Windows lifecycle/artifact deployment;
- direct Cloudflare MCP primary transport;
- GitHub DATA/CONTROL fallback/recovery transport;
- bounded public-market working set and responsive direct transport.

## Non-goals

- no second runtime/daemon/watchdog/scheduler merely for a feature;
- no generic shell or arbitrary OKX RPC;
- no generic strategy/rules framework before a real boundary requires it;
- no database/service merely for governance or experiments;
- no microservice/Kubernetes split;
- no duplicate collector/formula/state authority;
- no permanent macro/news/on-chain/alternative-data ingestion unless a falsifiable hypothesis requires it and point-in-time provenance exists;
- no speculative SBE/multi-exchange migration without measured need;
- no live production trading before the explicit final gate.

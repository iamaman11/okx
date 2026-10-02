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
5. **P0.3 named rate/backpressure domains** — ACCEPTED.
6. **Stage-1 final T1–T5 acceptance** — CURRENT.

The P0/P1 labels are capability groups, not a competing execution order.

Current Stage-1 final acceptance status:
- T1/T2 evidence is assembled from the accepted Stage-1 capability slices on the same product architecture;
- the final tested #173 head is content-identical to current main (0 changed files across the merge commit);
- the final Windows artifact exists and passed the required CI gates;
- strict T3 remains OPEN until that exact-tree artifact is installed and its provenance terminalizes through CONTROL;
- primary Cloudflare product surfaces remain live/fresh on the accepted P0.3 runtime;
- current blocker is CONTROL-path liveness, not a product/runtime correctness gap;
- Stage 2 must not start until the final T1–T5 record closes.

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

## Universal question coverage rule

Natural-language questions are **not** backend/API methods. The platform must not add one Rust operation or one MCP tool for every wording such as “top gainers”, “highest volume”, “largest funding”, “compare these markets”, or “which contracts changed most”.

The reusable read path is a **bounded typed analytical plan** executed by the existing Rust owners:

```text
user question
 -> ChatGPT semantic planning
 -> versioned bounded typed query plan
 -> existing observation/runtime facts
 -> allowlisted deterministic analysis operators
 -> bounded evidence result
 -> ChatGPT interpretation/explanation
```

The typed plan may compose only explicit product primitives:
- universe selection: instrument type, settlement currency, lifecycle/state and other normalized factual selectors;
- factual projection: allowlisted normalized market/reference/account fields;
- deterministic operators: filter, stable sort, top/bottom-K, bounded grouping/aggregation and explicitly supported time-window comparisons;
- versioned derived metrics owned by `okx-analysis`;
- explicit `as_of`, freshness/coherence policy, result limit and response budget.

This is **not** SQL, an arbitrary expression language, a raw OKX endpoint proxy or executable code. Every field/operator/metric is an enum/versioned contract with deterministic validation and hard bounds.

Architecture rule:
- a **new wording or combination** of already-supported facts/operators requires no backend change;
- a **new factual source** is added once to the factual layer;
- a **new calculation/metric** is added once to `okx-analysis` with deterministic tests;
- a **new mutation/safety semantic** remains an explicit dedicated capability and never enters the generic read planner.

The external MCP surface stays deliberately small. Prefer one stable universal read contract rather than metric-specific tools:
- `query_capabilities` returns the current query-contract/catalog version, supported field IDs, metric IDs + versions/units, operators and hard limits;
- `query` accepts one bounded analytical plan that declares the catalog version it was built against and fails closed on an unknown/stale capability or unsupported primitive.

This keeps MCP tool discovery stable as the metric catalog grows: adding a metric does not require adding a new MCP method. Existing coarse tools may remain as convenience/compatibility recipes, but should compile onto the same factual/analysis owners rather than duplicate formulas.

The first broad plan variant after Stage 1 should be a bounded market-universe scan (for example top/bottom-N by supported price change, volume, funding, basis or another accepted metric), rather than separate `top_gainers`, `top_losers`, `top_volume`, etc. tools. Coarse mutation/risk-policy/scenario capabilities remain dedicated where their contracts carry materially different authority or safety semantics.

Acceptance for a general analytical query capability must prove:
- whole-universe selection is bounded and rate-budget aware;
- stable deterministic ranking/tie behavior;
- no silent missing instruments or unreported truncation;
- freshness/provenance/metric version is present;
- result cardinality and payload size are bounded;
- unsupported fields/operators fail closed;
- no second collector, cache owner, scheduler, database or formula owner is introduced.

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
- one bounded typed market-universe scan that can answer cross-universe ranking/filtering questions without endpoint-per-question growth;
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

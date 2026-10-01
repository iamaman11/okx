# Roadmap and acceptance cursor

## Single current cursor

**#160 — Industrial trading platform roadmap**

The production baseline and post-MCP hardening are accepted. New work follows one canonical five-stage DAG and the repository-wide simplicity guard.

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

The final live activation gate is not a development stage. Production live trading remains fail-closed until separately authorized after Stages 1–5 pass.

## Repository Guard v1 — prerequisite to Stage 1 business work

The guard exists to preserve the accepted architecture while keeping development fast.

Machine-enforced:
- protected `main` keeps the existing required contexts;
- CI runs architecture dependency validation, portable Rust validation and Cloudflare MCP validation in parallel;
- the existing required `linux-core` context aggregates those results, so no branch-protection administration is needed to enforce the new checks;
- `Cargo.lock` + `--locked` remain mandatory;
- the Cloudflare package graph is locked and CI/deploy uses the committed lock;
- exact tested source tree and Windows artifact SHA-256 provenance remain mandatory.

Review-enforced:
- every nontrivial change names a concrete need/failure;
- an existing authoritative owner is preferred;
- structural complexity delta defaults to zero;
- every non-zero crate/task/store/transport/dependency/tool delta must be justified;
- superseded paths are removed rather than retained indefinitely;
- tests name the product/architecture failure they protect.

The executable dependency DAG lives in `scripts/check_architecture.py`. The current ownership model remains canonical in `docs/ARCHITECTURE.md`.

## Five development stages

### Stage 1 — TRUTH

Compatibility, standard-port OKX WebSockets, exchange clock/rate correctness, account hierarchy, balances, positions, orders, fills, bills, PnL/funding/fee evidence and reconciliation.

Exit: market/account facts are coherent, typed, fresh/provenanced and black-box accepted.

### Stage 2 — INTELLIGENCE + RISK

Market microstructure, derivatives structure, portfolio exposures, stress/scenario evidence and deterministic hard risk policy.

Exit: professional decision evidence is calculated locally in Rust and can be consumed without ChatGPT doing financial arithmetic.

### Stage 3 — SCIENTIFIC RESEARCH + REPLAY

Reproducible datasets/features, OOS/walk-forward research discipline, deterministic replay/backtest and paper/shadow strategy lifecycle.

Exit: strategies are scientifically testable without live money or hidden future data.

### Stage 4 — EXECUTION + TCA

Complete order/position lifecycle, uncertain-submit recovery, partial fills, amend/cancel/protective orders and execution-quality attribution.

Exit: mutation mechanics are physically accepted in OKX Demo Trading while production live trading remains disabled.

### Stage 5 — GOVERNANCE + OPERATIONS + LEARNING

Hard policy/kill behavior, attribution/scorecards, recovery, observability, supply-chain evidence and operational acceptance.

Exit: platform is industrial-ready with production live trading still disabled.

## Final live activation gate

Only after Stages 1–5 PASS:
- explicit operator decision;
- separate least-privilege production Trade credential;
- explicit live enable;
- tiny bounded canary;
- fresh risk/account/market evidence;
- successful reconcile/close/TCA;
- immediate disable path.

No CI or test may silently cross this gate.

## Accepted foundation retained

The following remain accepted foundations and are not reopened without concrete evidence:
- reference/public/private observation;
- deterministic analysis boundary;
- one Tokio runtime / one agent ownership model;
- durable execution ledger and fail-closed pre-enable boundary;
- verified Windows lifecycle/artifact deployment;
- direct Cloudflare MCP primary transport;
- GitHub DATA/CONTROL fallback/recovery transport;
- bounded public-market working set and responsive direct transport from #153.

Historical issues such as #113 remain evidence records, not the current planning cursor.

## Non-goals

- no second runtime/daemon/watchdog;
- no generic shell or arbitrary OKX RPC;
- no generic strategy/rules framework before a real boundary requires it;
- no database/service merely for governance;
- no microservice/Kubernetes split;
- no duplicate collector/formula/state authority;
- no live production trading before the explicit final gate.

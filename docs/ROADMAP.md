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

## Accepted Stages 1–2 and current Stage 3 cursor

Canonical execution order inside Stage 1:

1. **P0.1 WebSocket 443 compatibility** — ACCEPTED.
2. **P0.2 exchange clock discipline** — ACCEPTED.
3. **Venue/instrument-state execution gate** — ACCEPTED.
4. **Account + ledger truth** — ACCEPTED.
5. **P0.3 named rate/backpressure domains** — ACCEPTED.
6. **Stage-1 final T1–T5 acceptance** — ACCEPTED.

The P0/P1 labels are capability groups, not a competing execution order.

Stage 1 final acceptance is **ACCEPTED/CLOSED** on the exact tested/deployed tree:
- final tested head: `ec4359d388b8a71fba90fe360b986974f1d4fe96`;
- current merged main at acceptance: `6b7519e158207943d1cedce92d302e3c2d7d3c42`;
- compare tested head -> merged main: 0 changed files;
- final source tree: `481a9f2b4a9e4cf97b921dbe55c7e8be1f32924b`;
- final Windows artifact: `11200168727` from CI run `36941520981`;
- exact artifact installed and provenance verified through CONTROL;
- Cloudflare-primary market/account/capability paths physically passed on the installed binary;
- independent encrypted GitHub DATA fallback parity passed on the same binary;
- exact-tree restart advanced direct transport generation and market/account state reconverged to FRESH.

Stage-1 acceptance scope is the authenticated production execution account and the factual surfaces available through its least-privilege observer credential. A separate Main-account Read-only credential now exists and authenticates, but it is intentionally not connected to the production runtime; full main+subaccounts treasury inventory remains a deferred capability and `multi_account_inventory_complete=false` stays explicit until that capability is deliberately integrated and accepted.

**Stage 2 — INTELLIGENCE + RISK: ACCEPTED/CLOSED. Current roadmap cursor: Stage 3 — SCIENTIFIC RESEARCH + REPLAY.**

Stage 2 progress:

- **bounded universal analytical query — ACCEPTED/CLOSED**: `query_capabilities` + `query`, bounded whole-derivatives scan, stable top/bottom-K, explicit missing/excluded/truncation/coherence/provenance;
- **live microstructure core — ACCEPTED**: spread/depth/impact and sequence-contiguous live evidence through the existing bounded public-WS owner;
- **derivatives + history intelligence — ACCEPTED**: recent trades, realized-volatility/volume/OI change, funding history/regime, basis/term-structure evidence, plus production fixes for live OKX wire/rate-domain behavior;
- **portfolio risk mandate/hard-policy primary T4 — ACCEPTED**: PR #184 is on main `fe67d3495479e72a56c20c08fc5c231ed0670225`; exact tested head `74e14e5b1ff09c4fe27b1e7579eee9e905c2d843`; CI `37073209534` PASS; artifact `11256197440` deployed; Worker contract `okx.mcp.tools/2026-10-03.1`. After refreshing the connected tool schema, primary Cloudflare allow + intentional reject cases both passed with FRESH coherent evidence, typed policy rejection, non-null BTC reference generation in the reject case, and zero-residual OKX account-position-risk oracle comparison. GitHub fallback was not used as a substitute.
- **portfolio-risk correctness — ACCEPTED**: end-of-evaluation generation/coherence admission, explicit source-skew/oracle semantics, fail-closed unsupported daily-loss currencies and explicit mandate field semantics are merged/deployed;
- **statistical portfolio risk — ACCEPTED**: #187 adds explicit-sample covariance/correlation, volatility contribution, deterministic scenario/historical stress; Expected Shortfall remains explicitly NOT_COMPUTED until a declared valid tail-sample contract exists;
- **pre-mutation hard-policy ownership — ACCEPTED for implementation/fail-closed boundary**: #188 merged as main `1e7b37a7705d1925d9979e4c20cb63fdff4ae903`; tested head `093f489b07b894ada932b0f393c6f9ae1f2bf583`; tested/merged tree `9eafeaca84c3080edfcae813901c9b7b38d4eaaa`; CI `37087545928` PASS; artifact `11261092800` deployed/restarted with provenance; primary runtime and `portfolio_risk/v3` regression remained PASS/FRESH. This does not claim a live production mutation.
- **account-mode-aware Futures virtual risk + Stage-2 final acceptance — ACCEPTED/CLOSED**: PR #197 tested head `efa6f8dec36c3abde0d9abc69f749d4b75c0b05c`, tested/merged tree `b63f98d57df6e9e1e1b6fb1c24c949d580717603`, CI #843 / `37160080767` PASS, Windows artifact `11287008643` deployed with persisted provenance. Primary Cloudflare `portfolio_risk/v6` physically passed on Futures mode with a non-zero BTC+ETH COUNTERFACTUAL portfolio, FRESH mark/reference evidence, exchange-published contract constraints, statistical evidence, independent observed-account oracle consistency and zero exchange mutation.

Canonical Stage-2 closure order — **DONE / ACCEPTED**:

1. **DONE** — primary `portfolio_risk` allow + intentional reject through connected Cloudflare MCP.
2. **DONE** — portfolio-risk correctness: generation/coherence/skew/oracle semantics and explicit mandate fields.
3. **DONE** — statistical portfolio risk with explicit samples/windows, covariance/correlation, volatility contribution and deterministic stress.
4. **DONE** — immutable mandate/hard-policy revalidation immediately before the existing mutation boundary; production live trading remains disabled.
5. **DONE** — account-mode-aware exchange-oracle correction:
   - authenticated `acctLv=2` Futures mode no longer requires Position Builder;
   - COUNTERFACTUAL multi-instrument risk remains deterministic in `okx-analysis` over FRESH OKX mark/reference facts and exchange-published contract/lot/min/max/leverage constraints;
   - the real observed account remains independently checked against `account-position-risk`;
   - Position Builder remains typed for `acctLv=3/4` only;
   - no account-mode change, extra account/subaccount, Demo subsystem, second observer, fallback calculator, new transport or new MCP tool was introduced.
6. **DONE** — final Stage-2 T1–T5 acceptance:
   - tested head `efa6f8dec36c3abde0d9abc69f749d4b75c0b05c`;
   - tested/merged tree `b63f98d57df6e9e1e1b6fb1c24c949d580717603`;
   - CI #843 / run `37160080767` all jobs PASS;
   - artifact `11287008643`, digest `sha256:52d3521259ab2109f9a0d9781002f59c30272315d4771476e0a7603fc6010da2`;
   - merged main `f1028e97f984e313f9c0b702010ebeca4518a088`, merge tree == tested tree;
   - CONTROL deploy PASS, installed agent SHA-256 `1bdba04deb1f9a93761f7521279341d32dfaca883046df49da378aa9ac2b02d6`;
   - primary runtime PASS/session_fresh generation 114;
   - primary `portfolio_risk/v6` non-zero Futures proof PASS; hard-policy and exchange-constraint negative cases PASS; post-check account remains FRESH/coherent with zero positions/orders and consistent reconciliation.

Structural delta for the corrective slice: **0** new crates, long-lived tasks, mutable state owners, stores, schedulers/poll loops, transports, mutation authorities, MCP tools, credential models and third-party dependencies.

Cross-cutting CONTROL reliability debt discovered during item 5 is **DONE / ACCEPTED** in #193:
- merged main `239232ad9ef6308893c86c4afa1fbcf9fc6b6690`;
- exact tested head `053cb0213f4367113dd32aada83e9704e0a9b1cd`, tested/merged tree `b16c3d56438b01402f09a051b8092a43a3d22145`;
- CI #832 / run `37125627129` PASS;
- controller artifact `11274982057`, controller SHA-256 `10784b32ba24dc65058ff3b7c7c03783b28c1ae6e3726b2c85eeecdfc1d0004f`;
- existing `okx-github` now owns bounded 10s connect / 60s whole-request timeouts plus typed Timeout/Decode classification; no new owner/task/transport was introduced;
- controller stage/handoff committed through the immutable launcher, and `acceptance_crash_controller` physically proved durable-terminal-before-exit, automatic Scheduler/launcher recovery, no replay and post-recovery PASS/FRESH runtime;
- post-recovery production account remained FRESH/coherent with 0 positions, 0 pending orders and consistent ledger reconciliation.

Current operational baseline:
- canonical main `36c42c7cc7022f0c2ccaab2631ca4ce4b7609b4f`;
- Worker contract `okx.mcp.tools/2026-10-03.3`;
- direct runtime PASS / connected / session_fresh, generation 114;
- Stage 2 PASS/CLOSED;
- production live trading remains disabled.

**Stage 3 — SCIENTIFIC RESEARCH + REPLAY is now the current roadmap cursor.** Its implementation must continue to extend the accepted owners without reintroducing endpoint-per-question growth or a second product runtime.

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
- CONTROL `request_id` values are immutable single-publication identities: before any issue #12 POST, exact-search the candidate id in comments and abort if #12 already contains the request or terminal; controller deduplication remains defense in depth, not a retry mechanism;
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

Exit: factual market/account/reference truth for the authenticated production execution account is coherent, fresh/provenanced and black-box accepted. Treasury-wide main+subaccounts aggregation remains a separate external/deferred capability until a master read credential exists.

### Stage 2 — INTELLIGENCE + RISK

Market microstructure, derivatives structure, portfolio exposures, stress/scenario evidence, a versioned trading/research mandate and deterministic hard risk policy.

Important proof includes:
- decision-grade evidence with explicit coherence;
- one bounded typed market-universe scan that can answer cross-universe ranking/filtering questions without endpoint-per-question growth;
- depth/spread/impact/basis/carry;
- exact sizing/margin/risk boundaries;
- exchange-oracle differential checks against supported account-position-risk / max-size / position-builder evidence;
- end-of-evaluation reference-generation and bounded multi-source skew admission;
- hard daily-loss policy never silently disappears when currency conversion evidence is unavailable;
- mandate fields are either active constraints/derived evidence or explicitly context-only;
- the same hard policy is revalidated on fresh accepted evidence immediately before future mutation inside the existing execution owner;
- representative non-zero risk/oracle proof without enabling production live trading;
- no duplicate venue/reference/risk owner.

Exit: professional decision evidence is calculated locally in Rust, the same hard policy is enforceable at the mutation boundary, and ChatGPT does not become the financial calculator or safety authority.

### Stage 3 — SCIENTIFIC RESEARCH + REPLAY

Current development cursor. Stage 3 turns accepted Stage-1/2 truth/risk into a reproducible research system without creating a second platform.

Core architecture:
- one admitted new domain crate at most: `okx-research`;
- `okx-observation` remains the current factual owner;
- `okx-analysis` remains the deterministic feature/statistics/risk/strategy-formula owner;
- `okx-research` may own immutable datasets, replay, experiment lineage, validation and promotion artifacts only;
- no second Tokio runtime, daemon, scheduler/poll loop, operational database, transport or exchange mutation authority;
- no research-only duplicate implementation of a strategy intended for paper/live;
- a local content-addressed immutable artifact repository is permitted and is not a mutable live-status database.

Historical-data rule:
- prefer official OKX historical data/query sources where they provide the required facts;
- Tier A = candles/funding + explicit modelled execution costs;
- Tier B = provenance-locked trades/order-book/reference events;
- never claim historical L2 execution from Tier A;
- unavailable required event/reference history => `INSUFFICIENT_DATA`;
- historical replay must carry point-in-time universe/reference facts and availability-time semantics; the current `ReferenceRegistry` must not be used as a shortcut for historical eligibility/rules.

ChatGPT/product rule:
- ChatGPT formulates hypotheses and interprets compact evidence; Rust owns data/replay/statistics/risk;
- raw candles/trades/L2 do not traverse MCP for chat-side calculation;
- preserve current direct-transport limits (64 KiB frames, 20 s response deadline, existing bounded inflight);
- normal research summary target <= 12,288 bytes;
- long acquisition/replay uses deterministic bounded steps + immutable continuation/artifact ids, not a background job queue/poller;
- a fresh chat must be able to inspect/resume by experiment/artifact id;
- external research surface should remain at most `research_capabilities` + `research` unless a concrete typed-authority boundary proves another tool necessary.

Final pre-implementation invariants:
- causal timing is explicit: event -> availability -> decision -> earliest execution;
- Tier-A intrabar ambiguity is conservative/`AMBIGUOUS_FILL`/Tier-B, never guessed favorably;
- signal/mark/index/execution prices are distinct roles;
- funding is applied as historical events; fee/cost/capacity assumptions carry provenance;
- every human/ChatGPT/sweep trial belongs to immutable research-family lineage;
- final OOS is sealed before opening; descendants cannot reuse consumed holdout as pristine;
- sample adequacy can produce `INSUFFICIENT_EVIDENCE`;
- null/simple baselines prove replay accounting before alpha claims;
- dataset identity records canonical raw + normalized hashes;
- immutable pinned EvidenceStore is separate from quota-bounded evictable SourceCache;
- archive ingestion is bounded/untrusted/allowlisted;
- MCP freshness is separate from research-input freshness; ChatGPT/Cloudflare is never in local paper/shadow decision latency;
- PAPER = live facts + simulated execution; SHADOW = production-intended decision/risk path stopping at `WOULD_SUBMIT`;
- positive promotion to PAPER/SHADOW requires explicit operator authorization.

Canonical Stage-3 execution order — exactly four large slices:

1. **3A Research Data Foundation**
   - **v1 scope:** USDT linear perpetuals; BTC/ETH/DOGE; Tier-A 1H candles + funding + point-in-time reference, mark/index history only where required; one bounded BTC Tier-B source-schema proof;
   - explicitly not all OKX instruments/cadences/L2 in v1;
   - unknown historical reference coverage -> `INSUFFICIENT_REFERENCE_HISTORY`;
   - official-source historical acquisition/chunking;
   - immutable dataset/chunk hashes, gap/duplicate/out-of-order evidence;
   - Tier A/Tier B classification;
   - event-time + availability-time semantics;
   - point-in-time universe/reference timeline;
   - deterministic bounded resume/checkpoint semantics;
   - raw + normalized canonical hashes, pinned-vs-cache storage quotas and hardened archive ingestion.
   - **T1:** parser/raw+normalized canonical hash/gap/current-reference-poison/chunk-boundary/storage-quota/hostile-archive fixtures.
   - **T2:** identical source -> identical dataset hash; restart/resume equivalence; no second live owner; existing rate/backpressure preserved.
   - **T3:** exact build provenance + dataset binding to parser/normalization/source versions.
   - **T4:** primary MCP acquires/inspects one bounded real Tier-A dataset and, where available, one bounded Tier-B sample without bulk payload.
   - **T5:** corrupt hash/gap/unsupported source/stale transport fail closed; resume remains idempotent.
   - Exit: the application can prove exactly what historical data exists, provenance, availability and gaps.

2. **3B Deterministic Replay Kernel**
   - one `okx-research` replay owner;
   - versioned hypothesis + experiment spec;
   - deterministic event ordering/no-lookahead plus explicit event/availability/decision/earliest-fill causality;
   - typed signal/mark/index/execution price roles and conservative Tier-A intrabar ambiguity;
   - event-based funding and versioned fee/funding/spread/slippage/capacity model with provenance;
   - same `okx-analysis` features/strategy/risk semantics intended for shadow/live;
   - deterministic experiment hash/result.
   - **T1:** feature golden vectors, poisoned-future/same-close isolation, ordering, intrabar ambiguity, price-role, variable-funding, fee provenance, quantization/cost/capacity monotonicity and repeatability.
   - **T2:** captured-live replay parity where semantics match; no out-of-split reads; restart/resume same terminal hash; Stage-2 risk remains authoritative.
   - **T3:** experiment binds exact source/dataset/algorithm/mandate/risk/cost versions.
   - **T4:** primary MCP proves `NO_TRADE` + one manually auditable baseline before a real Tier-A experiment; Tier-B claims require inspected compatible Tier-B source semantics.
   - **T5:** future poison/gaps/unsupported Tier-B return typed failure or `INSUFFICIENT_DATA`; exchange mutations = 0.
   - Exit: one strategy can be replayed reproducibly without hidden future data or duplicate production formulas.

3. **3C Scientific Validation & Promotion**
   - chronological train/validation/untouched final OOS;
   - walk-forward;
   - purge/embargo only where overlapping horizons require it;
   - physically sealed final-holdout consumption lineage;
   - immutable research-family lineage for human/ChatGPT/sweep trials;
   - sample/evidence adequacy;
   - cost/capacity/parameter sensitivity and regime breakdown;
   - immutable negative-trial retention;
   - DSR by default where applicable; PBO/CSCV only where candidate-family/sample assumptions are satisfied;
   - immutable promotion + demotion/invalidation criteria and `PromotionBundle`.
   - **T1:** split/walk-forward/purge/embargo/DSR/PBO/overfit/all-trials/sample-adequacy/sealed-holdout fixtures.
   - **T2:** same immutable experiment family -> same validation decision; negative trials stay queryable; prior lineage cannot be rewritten.
   - **T3:** promotion bundle binds all exact hashes/versions/criteria.
   - **T4:** primary MCP returns one compact `REJECT | BACKTESTED | INSUFFICIENT_DATA` decision with OOS/walk-forward/cost/anti-overfit evidence.
   - **T5:** deliberate overfit/corruption/missing applicable anti-overfit evidence blocks promotion.
   - Exit: `BACKTESTED` requires predeclared reproducible scientific evidence.

4. **3D Paper/Shadow + Product UX + final Stage-3 acceptance**
   - states `RESEARCH -> BACKTESTED -> PAPER -> SHADOW`, with explicit operator authorization for positive promotion;
   - PAPER = live facts + simulated execution/virtual PnL; SHADOW = production-intended candidate/risk/pre-execution path ending at `WOULD_SUBMIT`;
   - one bounded event-driven research-session component inside the existing agent/runtime chain;
   - consumes existing accepted live observation; no second WS collector/full-market subscription/polling loop;
   - same feature/strategy/risk functions as replay;
   - append-only/content-addressed shadow evidence + compact status;
   - bounded/resumable MCP research UX, safe across transport loss and independent of chat history;
   - local paper/shadow remains outside MCP/ChatGPT latency and distinguishes transport freshness from research-input freshness.
   - **T1:** state transitions, replay/live-decision parity, <=12,288-byte normal summary, continuation idempotency.
   - **T2:** live observation feeds shadow without starving heartbeat/control/reconciliation; restart restores lineage; no exchange mutation path.
   - **T3:** exact Windows artifact/deploy provenance when agent changes; artifacts bind exact source/strategy/dataset versions.
   - **T4:** primary Cloudflare capabilities/run/resume/inspect-by-id + one real paper/shadow session; GitHub DATA parity only, never primary substitute.
   - **T5:** SESSION_STALE/reconnect, lost response, payload/time budget overrun, absent event history and corrupt artifact all fail/resume safely; production account unchanged.
   - Exit: one strategy lineage is traceable from falsifiable hypothesis through immutable data/replay/OOS/anti-overfit evidence to live paper/shadow with zero exchange mutation.

Stage-3 final acceptance must emit one bounded `okx.stage-acceptance/v1` over one exact accepted tree and include a simplicity review proving:
- <=1 new research crate;
- no new runtime/daemon/scheduler/operational DB/transport/mutation authority;
- no duplicate market/reference/formula/risk owner;
- direct transport limits were not enlarged;
- compact MCP evidence remains the chat boundary;
- production live trading remains disabled.

Exit: strategies are scientifically testable/reproducible and can progress through BACKTESTED/PAPER/SHADOW without live money or chat-history dependence.

**Stage 3A acceptance — PASS/CLOSED (2026-10-04).**

Accepted evidence:
- implementation PR #201; tested head `ab4ace3f3c46fdb414625660346587ae852fe912`;
- accepted source tree `5d9a73e385022e10792d9fa75b1c5b70f6aecdb6`, identical in merged main `2c248b945494e6012e22b3dd2b313fd87dfab3d5`;
- PR CI run `37206445119`: all required Linux, Windows, architecture and Cloudflare MCP jobs PASS;
- exact Windows bundle artifact `11305160431` installed by the existing controller; provenance persisted; agent restart PASS;
- production Cloudflare Worker deploy from merged main PASS; runtime remained PASS/connected/session-fresh on tool contract `okx.mcp.tools/2026-10-04.1`;
- primary Cloudflare MCP black-box `research_capabilities` PASS/FRESH and bound to the accepted source tree;
- primary Tier-A BTC/ETH/DOGE 1H inspections completed with immutable candle/funding/reference artifacts, zero explicit candle gaps, and the expected fail-closed `INSUFFICIENT_REFERENCE_HISTORY` verdict because a current instrument snapshot is not silently treated as historical point-in-time truth;
- primary Tier-B BTC historical-trades probe PASS/FRESH with 20 observed events, exact raw and normalized hashes, separate capture/chunk identities, explicit availability/continuity semantics, and no bulk rows returned through MCP;
- invalid Tier-B shape was rejected by the Worker and an out-of-range `trade_limit=101` was rejected by the connector schema;
- post-acceptance runtime remained PASS/FRESH; account remained coherent/reconciled with zero open positions and zero pending orders; Stage 3A has no exchange mutation authority.

Stage 3A exit is satisfied because the application can now prove what admitted historical data exists, its source/provenance/availability semantics, and where historical reference coverage is insufficient without inventing missing truth.

**Current cursor: Stage 3B — Deterministic Replay Kernel.**

The Stage-3 design freeze remains in force. New framework layers or methods require a reproduced source/runtime/test failure, not speculative completeness.

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

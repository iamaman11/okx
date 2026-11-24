# S0 functional coverage and migration obligations

**Status: IN_PROGRESS — not a release, installation, or Ready acceptance.**
Source baselines inspected: legacy `origin/main` `d4766e581822`, isolated
`origin/vnext` `7d0852f682ac`, and the current S0 proposal branch. The
normative requirements are [#569](https://github.com/iamaman11/okx/issues/569)
and the four owner schemas [Engine #571](https://github.com/iamaman11/okx/issues/571),
[Research #572](https://github.com/iamaman11/okx/issues/572),
[Control #573](https://github.com/iamaman11/okx/issues/573), and
[Edge #574](https://github.com/iamaman11/okx/issues/574).

This is an **evidence map**, not a claim that the old product was installed or
tested on a real host during this S0 change. `preserved` means the *cited
specific code/oracle* already exists under the proposed owner and is exercised
by that owner's test; it does **not** mean an entire U scenario passes. `planned`
means a target/legacy oracle is known, but the new end-to-end path has not been
proven. `missing` denotes a prerequisite or witness with no implemented new
counterpart. `retired` would require proof of no remaining consumer and the
same required behavior covered elsewhere: **none is asserted here**.

## U01–U08: one operation, one state/effect owner

| ID | Typed input → authority → expected output / effect | Legacy executable basis (main) | vNext evidence actually present | End-to-end status / next oracle |
| --- | --- | --- | --- | --- |
| **U01** | Versioned `StrategyPlan` A/B/C + same immutable candle digest → **Research** → three distinguishable numerical results, stable on replay; Edge only transports | [`okx-strategy-core/query_plan.rs`](../../crates/okx-strategy-core/src/query_plan.rs), [`research-service/analytics/research.rs`](../../crates/okx-research-service/src/analytics/research.rs), [`MCP`](../../cloudflare/okx-mcp/src/mcp.ts) | **preserved** numerical graph/replay kernel in [`research/query_plan.rs`](../research/crates/okx-strategy-core/src/query_plan.rs); standalone CLI [`tests/replay.rs`](../research/tests/replay.rs) now checks 3 data-only plans, one six-bar source, 3 numeric oracles and deterministic replay; S0 A/B oracle computes source SHA-256 | **planned**: external vNext MCP/API A/B/C route on immutable captured source and proof that data-only hypothesis adds **zero Rust build**; current CLI fixture alone does not satisfy U01 |
| **U02** | Named/operator version + typed parameters/units → **Research** → finite or diagnosed stream/replay result | [`operators.rs`](../../crates/okx-strategy-core/src/operators.rs), [`stream.rs`](../../crates/okx-strategy-core/src/stream.rs) | **preserved** kernel/operator tests in [`research/operators.rs`](../research/crates/okx-strategy-core/src/operators.rs), [`research/stream.rs`](../research/crates/okx-strategy-core/src/stream.rs) | **planned**: independent additive operator diff + recorded compilation/link/test events, artifact and 0 Engine activity not yet measured |
| **U03** | Public market source/instrument/timeframe → **Research** → data with `event_time`, `available_at`, coverage, gap, provenance/error and budgets | [`source_api.rs`](../../crates/okx-research-service/src/source_api.rs), [`analytics/sources.rs`](../../crates/okx-research-service/src/analytics/sources.rs), [`okx-api/public_data.rs`](../../crates/okx-api/src/public_data.rs) | **preserved** public candle adapter in [`okx_candles.rs`](../research/src/adapters/okx_candles.rs) plus causal closed-bar rejections in [`tests/replay.rs`](../research/tests/replay.rs) | **planned**: complete public REST/WS sources, catalog/jobs, quotas, durable data/provenance, real-provider source replay and independent source-extension build |
| **U04** | Checked plan + Observe mode + monotonic closed bars → **Research** (compute), **Engine** (observe/no POST) → identical causal decisions over replay and stream, restart checkpoint | [`trade_service/strategies.rs`](../../crates/okx-trading-engine/src/trade_service/strategies.rs) and [`research-service/analytics/decision.rs`](../../crates/okx-research-service/src/analytics/decision.rs) | **preserved** closed-bar replay with rejection of gap/duplicate/look-ahead and numerical mean in [`tests/replay.rs`](../research/tests/replay.rs); **preserved** typed `Hold` vs `Flat` in [`engine/lib.rs`](../engine/crates/okx-trading-engine/src/lib.rs) | **missing**: vNext Observe-to-Engine consumer and durable signal/ACK/restart pipeline; prove every closed-bar replay/live output parity, `0 POST` through real send boundary |
| **U05** | Authorized `ManualTarget` or strategy signal + account/instrument/evidence → **Engine** single writer/RiskBook → prepare, reserve, commit, **at most one** venue send, reconcile/settle or durable `UNKNOWN` | [`accounts_native_trade_contract_tests.rs`](../../crates/okx-trading-engine/src/accounts_native_trade_contract_tests.rs), [`accounts_native_execute_safety_contract_tests.rs`](../../crates/okx-trading-engine/src/accounts_native_execute_safety_contract_tests.rs), [`okx-trade-execution/ledger/sql/intents.rs`](../../crates/okx-trade-execution/src/ledger/sql/intents.rs) | **preserved** original finance SQL schema [`finance-unified-v1.sql`](../engine/crates/okx-trade-execution/schema/finance-unified-v1.sql), risk/execution crate and standalone tests; **preserved** strict validated command and real SQLite/WAL idempotent **PREPARE-only** [`sql_handoff.rs`](../engine/crates/okx-trading-engine/tests/sql_handoff.rs) | **missing**: vNext production entrypoint implementing authorize → reserve → commit → send-once → reconcile; no claim of live order capability; Demo/Real must not be inferred from PREPARE test |
| **U06** | Stop Research while an Engine position exists → **Engine** protection/reconcile continue → safe outcome; new unjustified risk fail-closed | [`trade_service/strategies.rs`](../../crates/okx-trading-engine/src/trade_service/strategies.rs), [`accounts_native_crash_contract_tests.rs`](../../crates/okx-trading-engine/src/accounts_native_crash_contract_tests.rs), [`okx-trade-execution/risk/book`](../../crates/okx-trade-execution/src/risk/book/mod.rs) | **preserved** financial owner/store/protection tests within [`engine/okx-trade-execution`](../engine/crates/okx-trade-execution/src/lib.rs); source-level independence from Research in [`engine/Cargo.toml`](../engine/Cargo.toml) | **missing**: real owner process with open position, Research kill/restart, continuous safety and latency witness; no native production process/installation to kill in this S0 candidate |
| **U07** | `Doctor`/status + operation ID → **Control** primary owner/stage/code, process identity, age/completeness and safe next action; Edge only delivers | [`host-controller/runtime.rs`](../../crates/okx-host-controller/src/runtime.rs), [`host-controller/lifecycle_executor.rs`](../../crates/okx-host-controller/src/lifecycle_executor.rs), [`launcher/lib.rs`](../../crates/okx-host-launcher/src/lib.rs) | **preserved** strict typed `ControlCommand`, `DiagnosticCode`, `ComponentStatus`, `EvidenceCompleteness`, `SafeAction` definitions and serialization negative tests in [`control/model.rs`](../control/crates/okx-host-controller/src/model.rs); **preserved** Launcher implementation [`control/launcher/lib.rs`](../control/crates/okx-host-launcher/src/lib.rs) | **missing**: new Control daemon wiring the types to live processes/DB, local-only measured `doctor` p95 ≤200 ms/30 calls, accurate failure-stage correlation and actual host diagnosis |
| **U08** | Verified Research artifact/manifest + requested version → **Control** stage/activate/rollback → Research changes, Engine PID/SHA/financial DB unchanged | [`host-controller/artifact/research_component.rs`](../../crates/okx-host-controller/src/artifact/research_component.rs), [`compose-verified-research-v2.cjs`](../../scripts/compose-verified-research-v2.cjs), [`prepare-research-release.ps1`](../../scripts/prepare-research-release.ps1) | **preserved** Launcher staging/activation rollback logic in [`control/launcher/lib.rs`](../control/crates/okx-host-launcher/src/lib.rs); **preserved** strict typed lifecycle commands in [`control/model.rs`](../control/crates/okx-host-controller/src/model.rs) | **missing**: actual vNext Controller execution/installer/rollback and physical Windows Research upgrade/rollback witness preserving Engine PID, SHA and data |

## Critical financial/property witnesses: preserve the **property**, not the old job name

| Required invariant | Representative legacy witnesses (not automatically vNext acceptance) | S0 evidence/status | Required executable replacement |
| --- | --- | --- | --- |
| Single financial SQLite owner, WAL/reopen, schema compatibility | [`account_store_tests.rs`](../../crates/okx-trade-execution/src/account_store_tests.rs) (`account_store_wal_has_one_checkpoint_owner_after_initialize_and_reopen`), [`finance-unified-v1.sql`](../../crates/okx-trade-execution/schema/finance-unified-v1.sql) | **preserved** owner code/schema and SQLite PREPARE/reopen test; schema blob matches `main` | Re-run on same new production EXE+DB format, exclusive writer, persisted writer epoch/fencing and shutdown/reopen |
| Financial idempotency; lost ACK never means safe repost | [`accounts_native_trade_contract_tests.rs`](../../crates/okx-trading-engine/src/accounts_native_trade_contract_tests.rs) (`risk_book_contract_native_public_execute_lost_ack_and_replay_send_once`); [`account_store_tests.rs`](../../crates/okx-trade-execution/src/account_store_tests.rs) (`lost_response_after_post_returns_unknown`) | **preserved** local stored-intent tests; **missing** new venue send/reconciliation adapter | New product request → actual (emulated) POST boundary → lose receipt → restart → durable `UNKNOWN`, exactly one POST, no optimistic completion |
| Actual OS crash and recovery, unknown state | [`accounts_native_crash_contract_tests.rs`](../../crates/okx-trading-engine/src/accounts_native_crash_contract_tests.rs) (`risk_book_contract_native_os_kill_after_create_recovers_original_unknown`) | **missing** vNext process-level equivalent despite preserved SQL library | Kill real new owner process at exact send/commit phases; recover without second POST; evidence from independent emulator and DB |
| Concurrency, common account risk, policy revisions | [`accounts_native_execute_safety_contract_tests.rs`](../../crates/okx-trading-engine/src/accounts_native_execute_safety_contract_tests.rs) (`risk_book_contract_native_execute_100_callers_one_send_policy_cas_and_payload_conflict`); [`accounts_native_multiaccount_reserve_contract_tests.rs`](../../crates/okx-trading-engine/src/accounts_native_multiaccount_reserve_contract_tests.rs) | **preserved** risk/SQL calculation source; **missing** new public gate with 100 callers and cross-account RiskBook reconciliation | Run scoped account/UID/policy CAS and simultaneous two-account admissions against new transport + one writer; assert safe conflict and exact budgets |
| Protection independent of Research and outside transport availability | [`account_protection_status_contract_tests.rs`](../../crates/okx-trade-execution/src/account_protection_status_contract_tests.rs), [`accounts_native_crash_contract_tests.rs`](../../crates/okx-trading-engine/src/accounts_native_crash_contract_tests.rs) | **preserved** protection internals; **missing** integrated shutdown test | Live owning Engine continues protection, reconciliation and permissible reductions with Research+Edge offline |
| Observe, Demo, Real remain distinct authorities | [`trade_service/strategies.rs`](../../crates/okx-trading-engine/src/trade_service/strategies.rs), [`accounts_native_trade_contract_tests.rs`](../../crates/okx-trading-engine/src/accounts_native_trade_contract_tests.rs) | **preserved** command target vs financial-send distinction in [`engine/lib.rs`](../engine/crates/okx-trading-engine/src/lib.rs); **missing** full mode-boundary tests | Assert Observe=0 POST, Demo end-to-end, Real rejected absent separate physical permission; no S0 test implies Real authority |
| Source causality and numerical replay/live equality | [`strategy-core/query_plan.rs`](../../crates/okx-strategy-core/src/query_plan.rs), [`strategy-core/stream.rs`](../../crates/okx-strategy-core/src/stream.rs), [`research-service/analytics/tests`](../../crates/okx-research-service/src/analytics/tests/tests.rs) | **preserved** vNext numerical replay and temporal negatives; **missing** installed streaming service | Pin same immutable source/data digest; compare output at each closed bar, gaps, duplicates, order, availability, operator versions and resource bounds |

## Four independently testable deployments: current truth

| Owner | What is independently executable **now** | What is **not** yet independently operational | Status |
| --- | --- | --- | --- |
| Engine | Isolated Cargo workspace, original finance risk+SQLite/WAL unit tests and typed PREPARE/reopen integration witness | Real new Engine daemon/EXE with full trade execution, private venue path, installed service, isolated upgrade/rollback | **planned** |
| Research | Isolated Cargo workspace and executable **recorded** JSON replay CLI, causal/numerical tests, public candle adapter | Full service with public REST/WS/API, durable jobs/caches, real A/B/C MCP flow, live source and compatible installation | **planned** |
| Control | Isolated Windows Launcher build with existing Launcher tests, pure typed controller model | New Control runtime/service, actual operator policy migration, install/diagnose/rollback of all four owners, live host proof | **planned** |
| Edge | Isolated Cloudflare Worker build/check, Miniflare/DO tests and owner-specific Engine/Research lanes | Production deployment proof, independent upgrade/rollback, Control policy move, 3-party typed ACKs and compatibility with newly installed owner services | **planned** |

Component CI green means **the cited isolated compilation/tests succeeded**;
it is neither proof that four production processes exist nor that a real
Research-only install/rollback preserves an Engine PID/SHA. Existing
`main` is still the installed reference; there must never be two active
financial writers during migration. No live order, install, restart or
financial data migration is authorized by this document.

## Immediate S0 closure requirements

1. Cross-link each U01–U08 expected typed input/output/error and owner to a
   single executable new product route; label unimplemented routes `missing`
   until they actually exist. Track the union of legacy tests, scripts, wire
   contracts, database schema and artifacts, not just Rust source files.
2. Reconcile the existing [`split-symbols.tsv`](../split-symbols.tsv) status:
   42 mixed source files → 88 cutpoints; **0 migrated, 54 planned, 34 pending**
   at first S0 inventory. None of its `UNVERIFIED` last-consumer markers can
   be interpreted as migrated or safe to delete.
3. Execute the actual SQLite/WAL (simulated venue) and Research replay examples
   under two independently exported topology candidates. Pin the same domain
   oracle but use a real meaningful source edit and record exact Cargo/Node
   build/test events and timing. Compare new vs old split by compiler
   invalidation and migration cost, not merely fixture duration.
4. Define owner-specific actual runtime/installation proofs and a compact
   integration proof. Run independent update/rollback preserving the other
   owners' executable hashes/PIDs; on production-facing test routes, new
   financial risk must fail closed.
5. Perform the protected Ready matrix with full scoped finance/OS properties
   and candidate artifact hashes. The ≤120 s warmed CI requirement and
   production readiness remain **NOT_PROVEN** until the exact SHA/tree and
   physical evidence are recorded in [#569](https://github.com/iamaman11/okx/issues/569).

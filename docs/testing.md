# Trade acceptance evidence map — #448 / #449 / #501

**Canonical:** [product #448](https://github.com/iamaman11/okx/issues/448), [execution #449](https://github.com/iamaman11/okx/issues/449), [bounded simplification #501](https://github.com/iamaman11/okx/issues/501). This is an evidence map, **not** a new suite, an authorization to trade, or a PASS certificate.

**Current candidate:** `fix/windows-clippy-required-495-20261107`, Draft PR #499.
**Accepted regression baseline: `32ee438ecdc132ab0058177d6e9fd631cd774be7`,
tree `174d9561168976a94942caa28ca80ff64b0d32b4`.**
[Required CI 42581922408](https://github.com/iamaman11/okx/actions/runs/42581922408)
completed SUCCESS, including release build. The
[matrix artifact](https://github.com/iamaman11/okx/actions/runs/42581922408/artifacts/13658004909)
contains all 26 distinct required/selected/executed IDs, 26 PASS, zero failures or
missing cases, 219 matching expected/actual assertions, 26 readbacks, and 90
connected F0–F8 crash trials reopening the original SQL roots. Some cases share
a cohort transcript; these are not 26 independent full campaigns.
The [Windows bundle](https://github.com/iamaman11/okx/actions/runs/42581922408/artifacts/13659351084)
was downloaded and its source SHA/tree, Cargo.lock and all three executable hashes
were independently verified. New changes require their own checks; this baseline
does not certify an untested descendant, installed workflow or REAL trading.
The earlier `0680e90f` baseline remains historical evidence in the sections below.

## #501 application ownership and refactor pass

The Rust types and actual call graph own the architecture. This table is a review
index, not an additional operation registry or runtime framework. All 14 public
`AgentOperation` variants have arms in `query::execute`; detailed action variants
remain exhaustively matched in their existing services.

| Area / crates | Owner and dependency boundary | Refactor decision |
|---|---|---|
| kernel, analysis, research | Canonical normalized facts and deterministic calculations; complete resolved production dependency graph | Move 34 shared model declarations and deterministic fee code into kernel. Observation reexports the same types; API-dependent constructors remain adapter functions. Enforce K10 for all three crates, including external transitive edges. |
| api, observation, ws, runtime | API signs/executes HTTP; observation normalizes venue inputs; WS/runtime own subscription state and freshness | Preserve these owners. API InstrumentType reexports the canonical model. Adapters depend inward on pure types; pure analysis no longer depends on observation/API. |
| protocol, agent query | Typed public request validation and exhaustive command routing | Narrow market handler dependencies to PublicWsHandle/AnalyticsService; separate pure microstructure response calculation from snapshot acquisition. |
| agent trading, execution | TradeService operations; AccountStoreHandle owns each account SQLite connection and atomic claims | Keep existing settlement/cleanup/unsent-release/submission owners. Locate terminal release in its own module with explicit imports; its cohort census still requires integrated evidence. No fake census test replaces that evidence. |
| agent accounts and sources | Registry verifies UID and owns source task shutdown; capture owns source normalization | Await aborted source JoinHandle before shutdown returns. Preserve safe typed failure category instead of flattening every account snapshot failure. |
| agent diagnostics | Bounded local account projections and SQL readers | Separate protection trace; expose event gaps/eviction and first observed blocker separately from current boundary. Remove the default diagnostic's remote RiskBook request; report no local risk snapshot honestly. |
| agent research jobs/artifacts | Jobs is the sole SQLite job catalog, Sources owns data access, CpuExecutor owns bounded compute | Keep existing owners and direct queue/idempotency/crash/result tests. No replacement job framework. |
| github, bridge-mcp, cli | Mailbox/MCP/CLI adapters call existing native commands | Keep transport adaptation; no second trading executor found in reviewed entry paths. |
| host-control, host-launcher | HostExecutor/Windows Job owns child lifecycle; launcher owns active controller | Consolidate three copied durable CONTROL ACK predicates into one private function; keep distinct operation modes, network polling and durable transitions. |
| Cloudflare RuntimeSession/RiskBook | Authenticated current-generation risk commands mutate one DO transaction owner | Keep current session/auth/transaction owners and composed tests. Synthetic native fixtures remain explicitly distinct from physical Native/WSS evidence. |

### Executable operation and background owner index

The public surface is the exhaustive `AgentOperation` enum in
`crates/okx-protocol/src/lib.rs`; `query::execute` has one arm per variant.
This index names that production owner and a real code-level test target. A
test listed as `partial` covers only the named boundary; it does not prove the
entire public operation or installed workflow. `No focused query-arm test
found` means exactly that; it is not a claim that the behavior is absent.

| Public variant | Production owner / next boundary | Existing named evidence | Known gap at this boundary |
|---|---|---|---|
| `ConnectAccount` | `query::execute` → `AccountRuntimeRegistry::connect` → `AccountBootstrapper` | `accounts::tests::prepared_account_rejects_unverified_identity_before_sql`; `recorded_two_account_bootstrap_opens_independent_sql_owners` (bootstrap/SQL, partial) | No focused test drives the public query arm through credential resolution and publication. |
| `Market` | `query::execute_market` → `AnalyticsService::market` → `MarketBootstrap` / existing research job owner | `analytics::market::tests::prepared_screen_8x100_latency_and_response_budget`; `market_source_scope_is_bound_to_native_public_client_environment` (partial) | No focused public-dispatch test covers the whole request-to-response route. |
| `MarketCapabilities` | `query::execute` → `AnalyticsService::market_capabilities` | `analytics::tests::market_capabilities_discloses_supported_operator_names_and_units` | No separate public-dispatch test. |
| `MarketMicrostructure` | `query::execute_market_microstructure` obtains admitted fresh WS snapshot; `microstructure_summary` is pure projection | `query::microstructure_test::microstructure_projects_exact_spread_and_sweep_without_io`; `microstructure_rejects_crossed_book_before_sizing`; `bounded_book_sweep_rejects_insufficient_depth` | Query helper tests do not prove live WS transport freshness; that remains owned by `PublicWsHandle` tests. |
| `Research` | `query::execute` → `AnalyticsService::research` → job catalog, source adapters, `CpuExecutor` | `analytics::tests::immutable_result_pages_and_restart_catalog`; `c17_verified_export_reads_all_page_checksums_and_exact_rows` | These prove job/artifact contracts, not an exchange research result being profitable or a trading signal being approved. |
| `Trade` | `query::execute` → `TradeService::trade_with_invocation`; the existing public `trade` entry forwards to the same implementation; action dispatch in `trade_service/mod.rs`, `execute.rs`, `reconcile.rs`, `protection_cleanup.rs`, `entry_settlement.rs` | Direct suites: `entry_settlement_direct_tests`, `protection_cleanup_direct_tests`, `unsent_release_tests`; integration cases T09/T10 | Terminal release still has no honest isolated orchestrator test; full cohort census proof stays in integrated cases. No direct test substitutes a fake census. |
| `Strategies` | `query::execute` → `TradeService::strategies`; worker loop `strategies::run` → `maintenance_turn` / `strategy_turn` | `strategies::tests::strategy_risk_budget_not_notional`; integration strategy cases under T09/T10 (see case records) | No complete direct per-action acceptance for Create/Update/Start/Stop and no proof that a generated strategy is profitable. |
| `Portfolio` | `query::execute` → `portfolio::portfolio` → selected account SQL owners | `portfolio::tests::summary_requires_complete_requested_window_and_fresh_account_nav`; `four_accounts_preserve_strategy_and_unattributed_finance_isolation` | Does not prove historical USD expense import for every managed account; incomplete source coverage remains partial. |
| `PortfolioScenario` | `query::execute` → pure `portfolio::scenario` calculation | `portfolio::tests::virtual_risk_has_no_exchange_side_effects_and_requires_psd` | Scenario inputs are user supplied; this is not a market-data backtest or exchange execution. |
| `Diagnostics` | `query::execute` → `diagnostics::diagnostics`; operation view reads the local projection; finance view reads the existing cycle trace; default account details still queue the SQL owner | `diagnostics::tests::default_diagnostics_never_claims_a_remote_risk_summary`; `operation_projection_reads_are_local_bounded_and_isolated`; `distinguish_sql_queue_deadlines_from_real_storage_outages` | Cleanup startup reconstruction restores only retained durable facts; ACK/initiator/history remain unknown where unrecorded. Public blocked-owner behavior and other families are not proved by the local getter benchmark. |
| `FindInstruments` | `query::execute` obtains `PublicWsHandle::reference_snapshot`; `find_instruments_response` maps the protocol enum and delegates filtering to `ReferenceRegistry::find_instruments` before the 50-row response limit | `query::instrument_search_tests::find_instruments_filters_type_currency_and_asset_before_public_limit` directly exercises the production helper with mixed SWAP/FUTURES, currencies, assets and more than 50 leading exclusions | The helper test does not cover asynchronous WS snapshot acquisition through the public dispatcher. |
| `MarketOverview` | `query::execute` → demand WS instrument → fresh WS snapshot; on warming only, existing REST market snapshot plus explicit degraded quality | `query::microstructure_test::healthy_ws_overview_omits_unbounded_depth_and_fits_mcp_envelope`; `accounts::tests::recorded_public_ws_frames_make_production_market_snapshot_fresh` (partial) | No focused test covers the overview's full fresh-WS and REST-degraded branches together. |
| `AccountSummary` | `query::execute` resolves one account and reads its latest snapshot with `ACCOUNT_MAX_AGE_MS` freshness check | `accounts::tests::recorded_ws_login_business_ack_and_six_history_pages_capture_both_sql_accounts` (source path, partial) | No focused test covers this query arm's missing/stale snapshot responses. |
| `MailboxTelemetry` | `query::execute` returns installation identity from `AccountRuntimeRegistry` | No focused query-arm test found | Read-only response has no independent delivery/telemetry persistence proof. |

#### Mutating subcommands below the public variants

`Trade` has `TradeRequest::{Capabilities, Preflight, Preview, Execute, Status,
Cancel, Close}`. `Execute` carries `TradeOperation::{Open, Add, Reduce,
Close, Replace}`; only the service dispatch determines the exact transition.
Direct entry settlement, protection cleanup, unsent release, SQL idempotency,
and single-send tests are named above. T09/T10 integration records remain the
proof for composed order, finance, source-census and RiskBook behavior.

`Strategies` has read-only `List`/`Status` and mutating `CapitalAssign`,
`Create`, `Update`, `Start`, and `Stop`. Every mutation stays inside
`TradeService::strategies` and account SQL; `CapitalAssign` also checks the
current governance revision. Direct complete per-variant lifecycle acceptance
is not established by the one pure risk-budget unit test.

Cloudflare's public `trade.*` tools are separately validated in
`mcp.ts::mcpApi.fetch` and are dispatched to the same native `TradeService`;
they are not a second executor. `accounts` governance changes are owned by the
single `RuntimeSession` Durable Object and `account_governance_store.ts`
transactions; `risk_control` frames reach the sole `risk_authority.ts` /
`risk_book.ts` mutation path only after current-generation engine hello.
`auth.test.ts`, `runtime_session.test.ts`, `risk_authority.test.ts`, and
`acceptance_composed.test.ts` prove those Worker boundaries with test storage
and fixtures. They do **not** establish a physical Native SQL + WSS + Worker
production deployment by themselves.

#### Background and recovery owners

| Background path | Sole owner / actual caller | Existing named evidence | Exact limit |
|---|---|---|---|
| Account source capture shutdown | `AccountRuntimeRegistry::shutdown` signals each `source_shutdown`; `stop_source_task` awaits graceful finish or abort-and-join | `accounts::source_shutdown_tests::shutdown_timeout_aborts_and_joins_source_task`; T10-11 cooperative capture shutdown | Direct test proves the helper cancellation/join boundary, not every source task's external-client teardown. |
| Trade maintenance/recovery | `TradeService::strategies::run` → `maintenance_turn` → `reconcile_restored` / bounded per-intent `status_intent` | T09.7 and T10-11 recovery case records; `accounts::tests::recovery_prepared_account_reopens_catalog_and_original_sql` | These are scoped recovery paths; they do not establish an installed application's real account restart/recovery acceptance. |
| Native↔Worker session | `RuntimeSession` is the DO/session owner; `webSocketMessage` authenticates generation and dispatches risk control; `webSocketClose/Error` reject pending work | `runtime_session.test.ts`; `risk_authority.test.ts`; `cloudflare_transport.rs::stale_native_risk_reply_cannot_grant_a_different_generation` | Worker unit/composed fixtures are not physical WSS/network proof. |
| Account SQLite | `AccountStoreHandle::start` owns one SQL thread/connection per account; accepted FIFO jobs execute through `call` | `writer_queue_backpressure_and_account_isolation`; `blocked_a_sql_owner_does_not_delay_b_cancel_receipt`; `two_account_100_parallel_sql_readonly_isolation` | Backup is a temporary read-only copy, not a second writer; these tests do not assert every application query is short. |
| Strategy research jobs | `AnalyticsService` submits to the existing catalog/source/CPU owners | `market_artifact_publication_and_cached_result_roundtrip`; `c17_cursor_survives_catalog_restart_and_is_not_transferable` | Result determinism/complete data evidence is bounded by each source's declared coverage. |

This index records observed owners and existing tests; it does not promise that
every listed public action is complete. Missing focused tests are explicit
gaps. Full-case rows below remain SHA-scoped integration evidence and cannot
be inferred from these unit/component tests.

Direct checks added or retained for this pass include microstructure exact
spread/sweep, trace contention/eviction/unknown lookup/first blocker, source
failure category, abort-and-join Drop witness, CONTROL ACK identity/schema/action
negatives, and the existing model normalization/analysis/research suites.

This pass does **not** close all #501 acceptance: causal invocation threading and
bounded startup reconstruction now cover protection cleanup, not every operation
family. Cross-family diagnostic coverage, performance measurements and installed
user workflows still need explicit evidence. Startup reconstruction reads existing
SQL receipts asynchronously; it does not reconstruct a lost ACK or initiator.
The volatile trace must never be described as a complete durable history.
S1-05 remains a separate whole-harness lifecycle criterion; the source-task
shutdown test proves only the production shutdown helper it actually invokes.

### Local validation of the combined #501 pass (09.11.2026)

- Windows MSVC/SDK: actual Rust compile, link and execution verified; the final
  `cargo test --workspace --locked -- --test-threads=4` passed **932 tests**, with
  7 existing special-purpose ignored tests. These include child-process/paired
  Worker fixtures; ordinary workspace success does not replace their CI gates.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`, rustfmt,
  strict K10 with negative self-test, SQL schema guard and runner self-test pass.
  Worker `npm run check` passed; Worker source was not changed in this pass.
- New real-registry restart coverage loads an existing orphan `POSTING` receipt
  through the startup task and original SQL owner, preserves unknown ACK/parent,
  and observes zero exchange POSTs. Direct SQL tests check the existing index,
  bounded scan, identity join, malformed/orphan rows and original DB reopening.
- Local validation caught a Windows stack overflow while constructing the large
  Native/Worker test future. Returning the async future directly was insufficient.
  The trade command now pins its state on the heap at the existing entry boundary;
  callers store a handle, checked by `command_future_layout_tests`. The unchanged
  standard-stack test and whole workspace pass. This entry test without a Worker
  handoff does not replace the full connected CI campaign.
- `diagnostics::tests::operation_projection_reads_are_local_bounded_and_isolated`:
  1,000 warm local projection reads, debug Windows x86_64, Intel i7-4770,
  **P95 0.0758 ms**. Test stdout emits all raw nanosecond samples. This measures
  one populated local projection, not public MCP dispatch, blocked-owner behavior,
  all operation families or complete #501 performance acceptance.

The combined candidate's required CI and bundle are now accepted on `32ee438e`
above. The following residual list is open; it is not implied PASS by full26.

### #501 implementation slice after `32ee438e`

The changes below are the current implementation slice, not a certificate that
the whole issue is complete. Required CI must validate its own final SHA.

| Boundary | Production implementation | Direct verification |
|---|---|---|
| Account storage explanation | `AccountStoreHandle::diagnostic_storage_snapshot`; existing SQL loop publishes bounded policy/stops/coverage/storage images; explicit `Diagnostics(view=storage)` reads them without SQL jobs | `account_worker::storage_projection::tests::`; `finance_trace::tests::public_finance_diagnostic_does_not_queue_behind_sql_or_trace` also holds SQL/trace/source locks and exercises public dispatch |
| Research explanation | `Jobs::diagnostic_readback` publishes committed metadata; explicit `Diagnostics(view=research)` avoids the catalog SQL mutex | `analytics::jobs::job_diagnostics::tests::`, including public dispatch, scope, redaction, contention, poison, restart and bounded indexed subset |
| Terminal release error context | `TerminalReleaseError` preserves reached stage/account/intent/revision/reservation; public pending keeps the original reason code | `trade_service::terminal_release::tests::`; these SQL/coverage tests do not replace positive full-cohort T09/T10 evidence |
| Strategy ownership | New prepare/reserve validate decision/plan/receipt ownership; `None/manual` and existing receipt-first replay remain supported | `account_store::tests::attribution::`; two direct failures reproduced before the fix, five PASS after it; immutable historical fixtures are inserted, never rewritten |
| Public query contracts | Existing `query::dispatch` and actual account/analytics/WS handles | `query::public_dispatch_tests::`: account missing/stale/invalid-time/healthy, instrument filters before limit, capabilities/mailbox and missing market source |
| Research grid report | Train-only selection; per-candidate OOS/fees×2 summaries; immutable pre-compute input manifest, verified export and reference-aware retention | `okx-research` strategy registry tests, analytics grid/provenance/export/retention tests; legacy artifacts without the new fields remain supported |
| Scenario timeout | `runScenario` aborts and joins before restoring case environment; unjoined child errors survive native formatting and stop the matrix | `node --test --test-reporter=tap scripts/trade-integration/scenario-lifecycle.test.mjs` (8 Windows tests); mandatory existing Windows job |
| SQL owner shutdown | `finish_owner` joins its pending backup before closing SQL and publishing termination | `account_worker::shutdown_tests::`: barrier success/error/panic, original writer lease/reopen |

The default detailed diagnostic still reads SQL. Fast views are explicitly
last-observed projections, never trading authority; gaps, ages and UNKNOWN
history remain visible. Research diagnostics are installation-scoped and reject
account routing. Background publishers continue independently of readers.

Local validation of this source slice (09.11.2026): workspace tests **967 PASS,
0 FAIL, 7 existing ignored**, including the three SQL backup shutdown cases;
research **16/16**, analytics **63 PASS / 1 ignored**, and Windows process
lifecycle **8/8**. Runner self-test22, strict K10/negative guard, SQL guard and
Worker check, workspace Clippy with `-D warnings`, and rustfmt pass. Ignored
connected fixtures still require their canonical CI.
The earlier 163 execution tests were before the three shutdown tests; the
combined execution result is **166 PASS / 2 ignored**.

`finance_trace::tests::public_finance_diagnostic_does_not_queue_behind_sql_or_trace`
also measures 1,000 native public storage requests, including response
serialization, while the actual SQL owner and source locks are held: debug
Windows x86_64, Intel i7-4770 reference local host, **P95 0.5252 ms**. Test stdout
emits all raw nanosecond samples. Zero additional SQL jobs/HTTP/risk commands
are asserted and a known policy value remains available. This is one blocked
storage boundary; it is not release-build K9 or all-family performance proof.

Remaining acceptance includes cross-family causal coverage, all required S1/S2
assertion mapping, research decision metadata and detailed candidate progress,
the complete MCP R-UX vertical, prescribed release K9 workloads, installed smoke
and actual integration. Abandoned pre-result research evidence is not swept by
a general orphan collector; this slice preserves that existing limitation.
Generic Windows process-tree termination cannot prove orphan ownership after
its leader exits. These direct tests and SQL backup join do not declare the
whole S1-05 complete. PHYSICAL/REAL remain separate gates.

### #501 revision 6: finite residual acceptance list

#### Regression found by combined CI `42594279304`

`e19e6651` failed 22/26: T09.6/.7 shared Add response lacked the expected intent,
T09.8 hit PRICE_FRESHNESS_DEADLINE, and T10-02 observed an additional venue POST.
The first two locally passed on the same code; that is not a fix or proof of
stability. New sanitizer tests retain only safe typed Add refusal fields and
integer comparisons, without raw responses or credentials.

T10-02 now explicitly invokes the actual public Status after publishing the
signed external 0.70 position while SQLite owns 0.50. Before the production fix
this reproduced **6 POSTs versus expected 5**: an extra `order` sell0.20, while
the new Add was locally denied as UNATTRIBUTED_EXCHANGE_POSITION/NOT_STARTED.
Status had already verified its own stop, then treated the aggregate denial
as that intent's protection failure and entered emergency policy.

The corrected branch retains a separate aggregate blocker and partial status
only for this external-exposure reason after independently verifying the own
stop. Global new-risk census remains Denied. Missing/invalid own protection,
previous durable incidents, other denial reasons and release proof are unchanged.
The strengthened raw-observer→Status→SQL/Worker scenario passed with no extra
POSTs or own protection incident. Existing T10-08 protection-loss recovery and
T09.8 exact budget-denial scenarios also passed locally without changing inputs,
GET budgets, TTL, command/case deadlines or monetary assertions. Two additional
assertions retain the protection/blocker outcomes in the final case artifact.
This does not establish the root cause of the earlier T09.6/.7 or price failure;
the next combined required CI is still necessary for the accepted candidate.

Checked against the baseline source: Cargo.toml has 15 workspace members, and
AgentOperation has 14 variants, all listed in the public index above. The crate
groups above include every member and the Worker; nested Trade/Strategies actions
and background owners remain separate checks, not additional top-level variants.
Source declarations and production call sites own the structure, not this table.

`reuse/test` means inspect the existing assertion before adding code. `refactor`
is justified only by a concrete boundary problem. `feature-if-missing` is an
existing #449 deliverable, not permission to add new products. Commands below
are existing component commands unless explicitly marked **planned**; a component
command does not prove the whole row. Nonzero test count and exact source SHA are
required. These rows are the remaining work list for E0–E6; do not start another
repository-wide audit or silently grow it with unrelated improvements.

| Requirement / stage | Owner and change kind | Given → When → required Then | Check and current status / dependency |
|---|---|---|---|
| R0 / E0: inventory | `AgentOperation`, `query::execute`, existing owner tables; reuse | Existing enum/crates/actions → compare call sites → no missing public/background effect owner | 14 variants / 15 crates / Worker enumerated; ownership review present. This is not per-action functional acceptance. New gaps must name one row below. |
| R1 / E1: fast diagnostic contract | `diagnostics::{diagnostics,account_diagnostics,finance_diagnostics}`, `finance_trace::readback`, `protection_trace`; refactor/test | Known facts, held SQL/trace/catalog lock or unavailable Worker → public Diagnostics → bounded scoped response with known facts or explicit gap, no explain-caused SQL/remote/effect | `cargo test --locked -p okx-agent --lib diagnostics:: -- --nocapture` reuses local tests only. **Planned:** public dispatch healthy/blocked-owner/authorization tests; NOT_PROVEN. Needs owner publication and preserved existing fields. |
| R1/R6 / E1: causal identity and restart | `query::dispatch_with_governance`, trade/source/job/host owners; reuse/refactor/test | Request/background invocation, then restart/eviction/contention → explain → real origin/stage/age and missing history, no invented ACK/parent | `cargo test --locked -p okx-agent --lib diagnostics::protection_trace:: -- --nocapture` covers cleanup only. Cross-family publication/context tests **planned**; NOT_PROVEN. No second ledger. |
| R2/R4 / E2: terminal release | `terminal_release`, `risk_bridge`, `AccountStore`; test/refactor only if required | Real SQL/census/Worker flow; stale/incomplete evidence or lost reply → production release → stage-specific pending or verified same-identity ack, no duplicate debit/release | `cargo test --locked -p okx-agent --lib trade_service::unsent_release_tests:: -- --nocapture` proves unsent only; terminal cohort composition is in full26. **Planned:** direct terminal orchestration/producer-consumer proof, NOT_PROVEN. Fake complete-census flags forbidden. |
| R3/R4/R6 / E2: public Trade and Strategies | `trade_service::{mod,plan,execute,reconcile,strategies}`, SQL owner; reuse/test, fix only reproduced defect | Each supported action and applicable replay/conflict/revision/manual-strategy case → same production dispatch → correct account/amount/effect count or rejection before effect | Existing settlement/cleanup/unsent suites and full26 are partial per-action evidence. **Planned:** missing public action and strategy lifecycle tests; NOT_PROVEN. Preserve semantic idempotency and actual owner validation. |
| R4/R5/R10 / E2: S1/S2 | `accounts::recorded_support`, source observer, semantic DTO/goldens, SQL/Worker; reuse/test | Read before/crossing cancel, fresh follow-up, invalid account/query/TTL; equivalent decimal/replay/conflict → actual consumer → preserved proof ordering, exact money, no extra POST | `cargo test --locked -p okx-agent --lib immutable_signed_snapshot_phases_follow_events_not_background_read_counts -- --nocapture` checks source scheduling, not all S1. Baseline full26 accepted. Exact S1-01..04/S2-01..08 assertion mapping and missing negatives **planned**, NOT_PROVEN. Historical S table below is not current PASS. |
| R6/R8 / E3: non-trading public routing | `query::execute`, accounts/WS/analytics/portfolio owners; reuse/test | ConnectAccount identity publication; AccountSummary missing/stale; MarketOverview fresh/degraded; search filters before limit → public dispatch → exact response/scope/quality | `cargo test --locked -p okx-agent --lib query:: -- --nocapture` covers helpers only. **Planned:** dispatch contracts also covering Market/Capabilities/Microstructure/Research/Portfolio/Scenario/Mailbox; NOT_PROVEN. No new router framework. |
| R7 / E3: pure extensions and job/artifact contracts | kernel/analysis/research, `analytics::{jobs,sources,result,executor}`; reuse/test | Existing extension, queue/cancel/restart/corrupt cursor/artifact → production owners → strict dependencies, bounded resources, deterministic output or precise failure | `python scripts/check_architecture.py --self-test` and `python scripts/check_architecture.py --k10-purity` PASS on baseline. `cargo test --locked -p okx-agent --lib analytics:: -- --nocapture` reuses C16/C17 components; `cargo test --locked -p okx-research` reuses pure tests. C13–C20 assertion/extension change-budget mapping remains NOT_PROVEN, not a rewrite request. |
| R7/R8 / E3: R-UX01..08 and analyst path | analytics registry/jobs/result, portfolio SQL projection, existing MCP; reuse/test/feature-if-missing | Fixed dataset/manifest/64 candidates/train metric/split/costs → validate/backtest/grid/result/decision/export/reopen and finance drilldown → exact provenance/counts/checksums, no research-caused trade/financial mutation | `cargo test --locked -p okx-agent --lib grid_result_includes_selected_train_test_and_equity -- --nocapture` and `cargo test --locked -p okx-agent --lib portfolio:: -- --nocapture` are component evidence only. **Planned:** redacted MCP vertical transcript and required metadata/decision feature audit; NOT_RUN. Historical expense completeness is a blocker only for the named financial gate requiring it. |
| R5/R9/R10 / E4: S1-05 and production shutdown | scenario driver/child owners, `accounts::shutdown`, research executor, host CONTROL; test/fix only reproduced leak | Success/error/timeout with saved stage receipt → bounded teardown → owned children/tasks joined/stores closed, primary failure retained, next run isolated | `cargo test --locked -p okx-agent --lib source_shutdown_tests:: -- --nocapture` proves one helper. **Planned:** outer success/error/timeout lifecycle tests with real children/Windows handles; S1-05 NOT_PROVEN. No kill of unrelated processes. |
| R9 / E4/E6: installed CONTROL | host-control/launcher/release/install owners; reuse/test | Correct or wrong hash/schema/action/instance and interrupted stop/start → actual installed matching bundle → exact health/version/ack, original financial SQL preserved | `cargo test --locked -p okx-host-control` and `cargo test --locked -p okx-host-launcher` are component commands. **Planned:** installed bundle smoke; NOT_RUN. Verified downloadable bundle is prerequisite evidence, not installed proof. |
| R10 / E5: performance and change cost | public native Diagnostics, existing CPU/SQL/source/CI owners; measure then fix measured bottleneck | Release reference host, prescribed warm/cold workloads and blocked owners → public requests → useful bounded diagnostics, K9 latency/memory/control/queue correctness | Local projection P95 0.0758ms is baseline microbench only. **Planned:** public per-family 1000-sample explain benchmark, normative K9 workloads, raw samples and CI timing breakdown; NOT_RUN. Three completed changes need measured direct-loop evidence; unavailable historical timings stay unmeasured. |
| R11 / E6: final acceptance and integration | coordinator, existing guards/required CI/PR chain/docs; reuse/test | Collected row evidence + integrated code → final required checks/bundle/installed verticals → all mandatory #501 criteria on actual accepted source/build | Baseline required CI/full26/219/90/bundle accepted only at header SHA. New slice local checks then one final required CI, artifact/hash verification and ancestry/full-diff review **planned**; NOT_RUN. #499 remains Draft; PHYSICAL/REAL separate. |

Completion uses the #501 §7 checklist: R0 inventory; R1 explanations/context;
R2/R3 trading; R4 SQL/risk; R5 facts; R6 routing/auth; R7 research; R8 account/
portfolio; R9 host; R10 guards/dev-loop/measurements; R11 combined acceptance.
All have a row above. Existing known good implementations are kept. A missing
assertion is not evidence of a missing feature; a missing feature is not repaired
by adding a diagnostic wrapper. Each new row assertion must be linked to the
implemented test and its exact-SHA result before acceptance.

### Historical full26 implementation index

The table is the **historical r8.3 operation index**, with individually updated
rows only where receipts are cited. It is not an aggregate current result.
Older A0/A1 checkpoints below remain historical, not competing working branches.

The 26 authoritative IDs are owned by `scripts/run-trade-integration.mjs --list`; all 26 `cases/<ID>.mjs` files exist. **File presence proves neither feature completeness nor scenario PASS.** Every result applies only to its cited SHA and scope. No production behavior is classified `NOT_FOUND` solely because it lacks a test.

**Existing command per row:** `node scripts/run-trade-integration.mjs --case <ID> --artifacts artifacts/a0-<ID>` (replace both placeholders with that row's ID); Windows Native acceptance remains required. Before the next full run: `node scripts/run-trade-integration.mjs --self-test` and `node scripts/run-trade-integration.mjs --list`. `CONNECT` means reuse an apparently sufficient existing adapter and verify on the exact SHA, not PASS in advance; `EXTEND` means a missing oracle/stimulus; `FIX` means inspect a reproduced failure. No IMPLEMENT decision is justified by the current search.

| ID | production path::symbol | existing test/command | CODE evidence | COMPONENT result | INTEGRATION result | exact gap / next observation | next action | SHA/run/artifact |
|---|---|---|---|---|---|---|---|---|
| `T09.0` | `crates/okx-agent/src/accounts.rs::AccountRuntimeRegistry::open; cloudflare/okx-mcp/src/risk_book.ts::installCampaignEntryProof` | [`cases/T09.0.mjs`](../scripts/trade-integration/cases/T09.0.mjs), `--case T09.0` | FOUND: native SQL A/B + risk opening | NOT_RUN | NOT_RUN | Confirm two account SQL owners, signed source and zero initial risk on this SHA | CONNECT | `f06383e` / no CI artifact |
| `T09.1` | `crates/okx-agent/src/trade_service/mod.rs::TradeService::trade; crates/okx-agent/src/trade_service/execute.rs::post_once` | [`cases/T09.1.mjs`](../scripts/trade-integration/cases/T09.1.mjs), `--case T09.1` | FOUND: public execute, status, fill/bill and Worker readback | NOT_RUN | NOT_RUN | One actual open/close/cancel route; signed fees and exactly three posts | CONNECT | `f06383e` / no CI artifact |
| `T09.2` | `crates/okx-agent/src/trade_service/plan.rs::plan; crates/okx-agent/src/trade_service/execute.rs::execute` | [`cases/T09.2.mjs`](../scripts/trade-integration/cases/T09.2.mjs), `--case T09.2` | FOUND: second native entry and Worker reserve | NOT_RUN | NOT_RUN | 0.30 lot admitted, upper 3.0015 and exact outbound count | CONNECT | `f06383e` / no CI artifact |
| `T09.3` | `crates/okx-agent/src/trade_service/reconcile.rs::settle_incremental_entry` | [`cases/T09.3.mjs`](../scripts/trade-integration/cases/T09.3.mjs), `--case T09.3` | FOUND: signed partial and terminal fill/bill sources | NOT_RUN | NOT_RUN | 0.12→0.30 fill, cumulative 4.002 and no retransmission | CONNECT | `f06383e` / no CI artifact |
| `T09.4` | `crates/okx-agent/src/trade_service/reconcile.rs::verified_live_protection; crates/okx-agent/src/trade_service/execute.rs::census_for_managed_protection_until` | [`cases/T09.4.mjs`](../scripts/trade-integration/cases/T09.4.mjs), `--case T09.4` | FOUND: managed add 0.20 and separate stop | NOT_RUN | NOT_RUN | Reproduce original protection incident with phase-aware inventory; prove owned/unowned stop distinction | EXTEND | `f06383e` / no CI artifact |
| `T09.5` | `crates/okx-execution/src/ledger/sql/intents.rs::by_idempotency; crates/okx-agent/src/trade_service/execute.rs::existing_request_hash_matches` | [`cases/T09.5.mjs`](../scripts/trade-integration/cases/T09.5.mjs), `--case T09.5` | FOUND: Native SQL + Worker semantic replay | NOT_RUN | NOT_RUN | Same-key replay/semantic changes preserve one decision/reservation/post | CONNECT | `f06383e` / no CI artifact |
| `T09.6` | `crates/okx-agent/src/trade_service/reconcile.rs::close; crates/okx-agent/src/trade_service/reconcile.rs::release_terminal` | [`cases/T09.6.mjs`](../scripts/trade-integration/cases/T09.6.mjs), `--case T09.6` | FOUND: close both lots; terminal and risk readbacks | NOT_RUN | NOT_RUN | Flat/orphan/held 0, entry and exit fees 0.003 each, 9 total posts | CONNECT | `f06383e` / no CI artifact |
| `T09.7` | `crates/okx-agent/src/trade_service/recovery.rs::reconcile_restored` | [`cases/T09.7.mjs`](../scripts/trade-integration/cases/T09.7.mjs), `--case T09.7` | FOUND: Native OS restart + persisted Worker readback | NOT_RUN | NOT_RUN | A99.994/B100 and no duplicate financial or venue mutation | CONNECT | `f06383e` / no CI artifact |
| `T09.8` | `crates/okx-agent/src/trade_service/execute.rs::revalidate_held_entry_quote; cloudflare/okx-mcp/src/risk_book.ts::reserveRisk` | [`cases/T09.8.mjs`](../scripts/trade-integration/cases/T09.8.mjs), `--case T09.8` | FOUND: budget denial after terminal cohort | NOT_RUN | NOT_RUN | Historical b93 failure in T09.4; distinguish source timing from actual stop; then upper4.002 vs remaining3.997 yields 0 new POST | FIX | `f06383e` / no CI artifact |
| `T09-MA` | `crates/okx-agent/src/trade_service/mod.rs::TradeService::trade; cloudflare/okx-mcp/src/runtime_session.ts::dispatch` | [`cases/T09-MA.mjs`](../scripts/trade-integration/cases/T09-MA.mjs), `--case T09-MA` | FOUND: A/B public barrier, scoped SQL and shared Worker | NOT_RUN | NOT_RUN | Confirm separate strategy/account identities, concurrency and shared limit without wrong-account mutations | EXTEND | `f06383e` / no CI artifact |
| `T09-SPEC` | `crates/okx-agent/src/trade_service/plan.rs::verify_linear_usdt_entry_units` | [`cases/T09-SPEC.mjs`](../scripts/trade-integration/cases/T09-SPEC.mjs), `--case T09-SPEC` | FOUND: second-spec public plan and Execute probe | NOT_RUN | NOT_RUN | ETH-USDT-SWAP alternate tick/lot/multiplier; invalid step 0 POST | CONNECT | `f06383e` / no CI artifact |
| `T10-01` | `crates/okx-agent/src/trade_service/execute.rs::verify_post_held_entry_sources` | [`cases/T10-01.mjs`](../scripts/trade-integration/cases/T10-01.mjs), `--case T10-01` | FOUND: stale/partial source plus fresh recovery probe | NOT_RUN | NOT_RUN | Complete source matrix (balance/history/algos/fee/FX/spec) before and after HELD with typed denial and recovery | EXTEND | `f06383e` / no CI artifact |
| `T10-02` | `crates/okx-agent/src/trade_service/execute.rs::certified_managed_external_exposure` | [`cases/T10-02.mjs`](../scripts/trade-integration/cases/T10-02.mjs), `--case T10-02` | FOUND: signed external 0.70 vs owned 0.50 | NOT_RUN | NOT_RUN | Positive owned exposure allowed; unowned/UNKNOWN blocks add and keeps money | CONNECT | `f06383e` / no CI artifact |
| `T10-03` | `crates/okx-agent/src/risk_bridge.rs::reserve_then_fence; cloudflare/okx-mcp/src/risk_authority.ts::handleNativeRiskCommand` | [`cases/T10-03.mjs`](../scripts/trade-integration/cases/T10-03.mjs), `--case T10-03` | FOUND: lost POST_AUTH reply, Worker INSPECT | NOT_RUN | NOT_RUN | Cover reserve-commit lost reply AND begin-post committed lost reply; same identity, no extra grant/send | EXTEND | `f06383e` / no CI artifact |
| `T10-04` | `crates/okx-agent/src/trade_service/execute.rs::risk_receipt_bound; crates/okx-agent/src/trade_service/execute.rs::post_receipt_bound` | [`cases/T10-04.mjs`](../scripts/trade-integration/cases/T10-04.mjs), `--case T10-04` | FOUND: foreign Worker command reply binding probe | NOT_RUN | NOT_RUN | Wrong command/account/reserve/plan/policy/CAS/generation all reject with 0 POST | EXTEND | `f06383e` / no CI artifact |
| `T10-05` | `crates/okx-agent/src/trade_service/execute.rs::post_once; crates/okx-agent/src/trade_service/reconcile.rs::status_intent` | [`cases/T10-05.mjs`](../scripts/trade-integration/cases/T10-05.mjs), `--case T10-05` | FOUND: lost ACK vs signed venue rejection cohorts | NOT_RUN | NOT_RUN | UNKNOWN then same clOrdId readback without resend, distinct explicit reject | CONNECT | `f06383e` / no CI artifact |
| `T10-06` | `crates/okx-execution/src/ledger/sql/intents.rs::prepare; crates/okx-execution/src/ledger/sql/intents.rs::by_idempotency` | [`cases/T10-06.mjs`](../scripts/trade-integration/cases/T10-06.mjs), `--case T10-06` | FOUND: 100 concurrent public identical-key requests | NOT_RUN | NOT_RUN | One intent/reserve and ≤1 create; changed payload conflicts | CONNECT | `f06383e` / no CI artifact |
| `T10-07` | `cloudflare/okx-mcp/src/risk_book.ts::reserveRisk; cloudflare/okx-mcp/src/risk_book.ts::releaseUnsentRisk` | [`cases/T10-07.mjs`](../scripts/trade-integration/cases/T10-07.mjs), `--case T10-07` | FOUND: Worker 3×4 plus Native shared-campaign prerequisite | NOT_RUN | NOT_RUN | Case intentionally FAIL: same-campaign Native source-backed 8→4 release, remaining 2→6 and no false SQL bridge | EXTEND | `f06383e` / no CI artifact |
| `T10-08` | `crates/okx-agent/src/trade_service/reconcile.rs::emergency_exit; crates/okx-agent/src/trade_service/reconcile.rs::verified_live_protection` | [`cases/T10-08.mjs`](../scripts/trade-integration/cases/T10-08.mjs), `--case T10-08` | FOUND: signed partial fill and protection first leg | NOT_RUN | NOT_RUN | Case intentionally FAIL: same-intent failed protection, late fill and bounded protective attempt | EXTEND | `f06383e` / no CI artifact |
| `T10-09` | `crates/okx-agent/src/trade_service/reconcile.rs::cancel; crates/okx-agent/src/trade_service/reconcile.rs::close` | [`cases/T10-09.mjs`](../scripts/trade-integration/cases/T10-09.mjs), `--case T10-09` | FOUND: B public Cancel ACK and persisted worker first leg | NOT_RUN | NOT_RUN | Case intentionally FAIL: same-intent post-ACK late fill, partial close and remaining protection | EXTEND | `f06383e` / no CI artifact |
| `T10-10` | `crates/okx-execution/src/ledger/sql/intents.rs::bind_late_entry_settlement; cloudflare/okx-mcp/src/risk_book.ts::settleEntrySpend` | [`cases/T10-10.mjs`](../scripts/trade-integration/cases/T10-10.mjs), `--case T10-10` | FOUND: duplicate fills/bills initial causal leg | NOT_RUN | NOT_RUN | Case intentionally FAIL: same-campaign late fee/rebate, replay revision and upper breach | EXTEND | `f06383e` / no CI artifact |
| `T10-11` | `crates/okx-execution/src/ledger/sql/intents.rs::recover_posting; crates/okx-agent/src/trade_service/submission.rs::post_once; crates/okx-agent/src/trade_service/reconcile.rs::observe_intent_order; crates/okx-agent/src/source_capture.rs::capture_account_sources` | [`cases/T10-11.mjs`](../scripts/trade-integration/cases/T10-11.mjs), `--case T10-11` | 90 connected F0–F8 kills; separate component evidence excluded from that count | COMPLETE FOR T10-11 | targeted PASS; full26 pending | Preserve exact owner/crash proofs in final required gate | REGRESSION | `a8afc8dd` / CI `42564173457`; scope below |
| `T10-12` | `cloudflare/okx-mcp/src/risk_book.ts::reserveRisk; cloudflare/okx-mcp/src/runtime_session.ts::dispatch` | [`cases/T10-12.mjs`](../scripts/trade-integration/cases/T10-12.mjs), `--case T10-12` | FOUND: Worker kill/reopen persisted DO after reserve | NOT_RUN | NOT_RUN | Case intentionally FAIL: after-settlement kill, fresh generation, Native binding/no double debit | EXTEND | `f06383e` / no CI artifact |
| `T10-13` | `crates/okx-agent/src/trade_service/reconcile.rs::close; cloudflare/okx-mcp/src/risk_authority.ts::handleNativeRiskCommand` | [`cases/T10-13.mjs`](../scripts/trade-integration/cases/T10-13.mjs), `--case T10-13` | FOUND: Worker STOP and protective close probe | NOT_RUN | NOT_RUN | Assert before-claim and post-fill pause ordering, stale reduce blocker and safe reduction | EXTEND | `f06383e` / no CI artifact |
| `T10-14` | `crates/okx-agent/src/trade_service/mod.rs::TradeService::trade; crates/okx-agent/src/trade_service/reconcile.rs::cancel` | [`cases/T10-14.mjs`](../scripts/trade-integration/cases/T10-14.mjs), `--case T10-14` | FOUND: A held SQL; B public Status/Cancel | NOT_RUN | NOT_RUN | Independent B progress under held A, then join A, 1 B cancel POST | CONNECT | `f06383e` / no CI artifact |
| `T10-15` | `crates/okx-agent/src/trade_service/execute.rs::revalidate_held_entry_quote; crates/okx-agent/src/trade_service/execute.rs::verify_post_held_entry_sources` | [`cases/T10-15.mjs`](../scripts/trade-integration/cases/T10-15.mjs), `--case T10-15` | FOUND: signed fee mutation after HELD | NOT_RUN | NOT_RUN | Check FX/spec/fee/policy changes and fixed plan binding before POST; 0 send on mismatch | EXTEND | `f06383e` / no CI artifact |

## A1 baseline and evidence boundary

The b93be8e Windows failure is in the T09.4 Add/Status prefix of T09.8: `INTENT_EXPOSURE_REMAINS`, correlated with `UNATTRIBUTED_CONDITIONAL_ORDER` and `COHORT_UNRESOLVED_POST_OUTCOME`. **Root cause not proven.** For A1 capture account/intent/source generation, route+query, phase ordering, two stop IDs, send/ACK, SQL/Worker revision; separate fixture publication timing from genuine unowned exposure. Use a deterministic I/O barrier; do not relax money checks, source freshness or production deadlines. The targeted positive must end in consumed 6.003, remaining 3.997, denied 4.002 and zero extra create POST. The negative must keep held/denied for incomplete evidence. Preserve baseline artifacts before changing code.

Known deliberate non-PASS cases on `f06383e`: `T10-07`, `T10-08`, `T10-09`, `T10-10`, `T10-11`, `T10-12`. Each writes or requests a partial prerequisite and throws a precise error until the integrated oracle is complete; a component probe cannot be substituted for this step. Remaining adapters also require runner validator/evidence sufficiency review and exact-SHA Windows execution.

## S1/S2 and USER acceptance (separate from 26 cases)

| Scope | Evidence already located | Exact outstanding / next verification | Status on `f06383e` |
|---|---|---|---|
| S1-01/02 | `accounts.rs::recorded_support`, `trade_integration_t09_4.rs`, `trade_integration_t09_6.rs` | Explicit pre-cancel and crossing-cancel GET barriers, stale then fresh inventory with original timestamp, source/budget accounting | NOT_PROVEN |
| S1-03 | Recorded support + transport invalid-request tests | Account/query/extra POST/missing event/expired source each independently fails with first reason and no mutation | NOT_PROVEN |
| S1-04 | Existing T09.0–.8 public suite; historical b93 CI | All nine T09 on one exact commit, no executed fail; exact source-backed budget/flat/restart | NOT_RUN |
| S1-05 | Existing Native test child lifecycle | Abort/join/close stores and preserve per-stage receipts on success/failure/timeout | NOT_PROVEN |
| S2-01/02/03/04/06/07/08 | Native semantic DTO + Worker `risk_book.ts::normalizeDecimal`, goldens, SQL replay tests | Locate exact current test filter and bind golden fixture + persisted replay to this SHA | NOT_RUN |
| S2-05 | Historical Windows policy-CAS artifact from b93 CI | Repeat actual immutable policy/strategy retry versus new-key decision on release SHA | NOT_RUN |
| R-UX01–08 / analyst / manager | `crates/okx-agent/src/analytics/*`; public MCP is the intended boundary | Run validate→backtest→grid/OOS→result→decision→export→reopen on one dataset and save checksum | NOT_RUN |
| Trader / INSTALLED / PHYSICAL / REAL | Existing P0 runtime, governance, release and backup owners | Only after CODE and applicable release/operational gates; REAL requires separate explicit authorization | NOT_RUN / NO-GO |

**Update rule:** refresh row evidence from the next exact SHA + CI/artifact; do not copy historical PASS across commits or turn a deliberate negative into a green gate. Required mandatory milestones remain 16→24→26 PASS with 0 executed failures. No full CI was triggered for this documentation/metadata update.
## #495 — entry settlement (bounded structural refactor only)

The coordinator now lives at `trade_service/entry_settlement.rs::settle_incremental_entry`,
with one production caller in `reconcile.rs::status_intent`. The old method
is removed; `release_terminal` remains the independent terminal owner. The
SQL owner still controls source-backed finance/bind/ack and NativeRiskClient
still carries authenticated Worker CAS. No changed FX, fees, risk limits,
network GET budgets, release ordering, or wire results.

**Direct Windows command** (run on final source SHA, require nonzero tests):

```text
cargo test --locked -p okx-agent --lib trade_service::entry_settlement:: -- --nocapture
```

R-01/R-02: `r01_r02_partial_settlement_and_same_revision_repeat_use_one_sql_ack`;
`r02_terminal_replay_reads_immutable_receipt_without_second_settle`.
R-03: `r03_released_late_bill_requires_same_reservation_new_revision_and_ack`.
R-04: `r04_missing_source_cannot_ack_and_identity_mismatch_preserves_first_reason`.
R-05: `r05_lost_settlement_reply_keeps_unacked_revision_and_replays_same_proof`.
R-06: `r06_three_cas_conflicts_never_ack_unaccepted_settlement`.
Additional tests prove demo/no-order fail-closed and typed error metadata.
All use real SQLite; programmable NativeRiskClient replies prove orchestration,
not Worker monetary arithmetic. No setup stamps successful settlement ACK
into a table.

[Windows run #42392615185](https://github.com/iamaman11/okx/actions/runs/42392615185)
at `6628b2acfb31d2f3bcdc8b4841760d5f366a2016` and
[run #42393552274](https://github.com/iamaman11/okx/actions/runs/42393552274)
at `6de0455f6930feced26a518df304945dfce657d1` both confirmed
**8 PASS, 0 FAIL** direct tests; the first timed **1.34s** after compilation.
Rust format/compile, architecture, Cloudflare and B1 prerequisites succeeded.
Later #499 run [#42403496577](https://github.com/iamaman11/okx/actions/runs/42403496577)
at `034f549cc9290e23479a8207dd2395ff1d2dbdc6` passed 8/8 direct
settlement and required Windows Clippy, but full trading acceptance was 20/26.
This does not establish release acceptance.

**Exact-parent address comparison**, original `3a892c4993e4a62a270197c52cd9d3c1afd00663`
against `6de0455f6930feced26a518df304945dfce657d1`:
- T09.3 PASS/PASS (run #42393552274, artifact 13582019224).
- T09.8 FAIL/FAIL at identical first
  `PROTECTION_CANCEL_OUTCOME_UNKNOWN_INSPECT` in `trade_integration_t09_6.rs:618`
  (artifact 13583265306).
- T10-09: first pair baseline PASS/candidate FAIL at recorded fixture
  `TRANSCRIPT_SOURCE_PAGE_UNEXPECTED_OR_EXHAUSTED` on
  `orders-algo-pending` (artifact 13583030900). On same SHA, one targeted job
  retry reproduced baseline FAIL/candidate FAIL with identical first
  `T10_09_AMEND_ACK_SQL_MISSING` and
  `PROTECTION_SHRINK_ORIGINAL_RISK_UNVERIFIED` in
  `trade_integration_t10_09.rs:846` (artifact 13582866530).
  Therefore the recorded integration path is **nondeterministic** even on
  the original SHA; it is **not** proven PASS or financial-complete.
- The operation bodies were compared with normalized whitespace after only
  replacing `self` dependencies, deadline provider, and successful typed
  outcomes; the transformed original and extracted source were identical.
  Counts before/after: 8 SQL calls, 5 risk action uses, 2 bounded retry loops,
  3 bind calls, 2 SQL ACK calls.

The temporary three-case Windows comparison matrix was intentionally removed
after preserving the two independent runs and artifacts. Only fast direct
production-boundary tests remain in the existing `windows-native` job.
Source-fixture stability and protection failures belong to #449; #495 cannot
be used to claim T10-09 PASS or bypass full26/required release gates.

**#500 protection cleanup — accepted bounded production operation.**

Acceptance proof is the completed Windows [run #42479784285](https://github.com/iamaman11/okx/actions/runs/42479784285)
on **source SHA `8cbcd554361455e80d51e69f564edc5c28820a89`**;
[26-case artifacts](https://github.com/iamaman11/okx/actions/runs/42479784285/artifacts/13617249020).
Exactly **23/26** are PASS on that SHA, including **T09.6, T09.7,
T09.8, T10-08 and T10-09**. Only T10-10, T10-11 and T10-12 FAIL;
these are subsequent #449 stages and are NOT acceptance criteria for #500.
Windows cargo tests, Clippy, Linux Rust/Core, Architecture, Cloudflare,
Windows B1 and both fast operation suites passed on the same source SHA.
Neither general CODE 26/26 nor REAL/installed release is accepted.

**One production operation, no shadow test algorithm.** Public `Status` and
PC-07 both invoke `trade_service/protection_cleanup.rs::cleanup_flat_protection`
with explicit SQL owner, authenticated observer, real REST adapter, semaphore,
UID and verified order facts. The old Status-local cancellation/ACK/readback
branch no longer acts as a second decision path; the duplicated ACK verdict
was consolidated in `send_protection_cleanup_after_admission`, and the same
SQL/observed readback/receipt path is called by the direct test. The existing
AccountStore remains the sole durable BEGIN/single-send/receipt owner;
release/census and money remain separate owners. A flat position with a
still-live original entry order is NOT cleanup permission. A lost ACK cannot
cause another POST; incomplete/stale, wrong-account or still-live algo reads
remain pending/blocked, not falsely confirmed. No financial formulas,
RiskBook authorization, TTL, wire/SQL schema or POST retry policy were relaxed.

**Causal RED → GREEN:** earlier [run #42417714196](https://github.com/iamaman11/okx/actions/runs/42417714196)
recorded `cleanup_<client-id>` SQL `POSTING` and an actual cancel attempt
for `900000002` **before the signed T09.6 cancel observation phase**. This
explained the later missing target ACK without granting resend permission.
The bounded fix aligned source/operation boundaries while preserving
independent signed pre-ACK and post-ACK observations, exact-algo POST/ACK
identity and the original durable operation. A production+fixture cause
must not be collapsed into permission to manufacture a successful readback.
The final T09.6/.7/.8 PASS plus PC-01–07 proves this bounded flow;
intermittent unrelated managed-Add readiness on other SHAs is not evidence
of an ACK or a financial regression in cleanup.

**Direct reproduction on the code SHA** (after building Rust on Windows):

```text
cargo test --locked -p okx-agent --lib trade_service::protection_cleanup::protection_cleanup_direct_tests:: -- --nocapture
cargo test --locked -p okx-agent --lib trade_service::entry_settlement:: -- --nocapture
node scripts/run-trade-integration.mjs --case T09.6 --artifacts <existing-artifact-dir>
node scripts/run-trade-integration.mjs --case T09.7 --artifacts <existing-artifact-dir>
node scripts/run-trade-integration.mjs --case T09.8 --artifacts <existing-artifact-dir>
node scripts/run-trade-integration.mjs --case T10-08 --artifacts <existing-artifact-dir>
node scripts/run-trade-integration.mjs --case T10-09 --artifacts <existing-artifact-dir>
```

In the exact CI run the direct protection tests **PC-01–07 = 7 PASS/0 FAIL
in 3.06s** *after compilation*. PC-01 covers one exact send/receipt and
replay; PC-02 lost ACK and no second send; PC-03 stale/incomplete sources;
PC-04 foreign identity/late exposure; PC-05 original SQL after restart;
PC-06 two callers and pre-ACK stale read; PC-07 full signed HTTP
cancel/ACK, independently read-back complete 16-scope inventory and same
production SQL receipt. Entry settlement **8 PASS/0 FAIL in 2.68s** after
compilation. The direct tests are registered in the existing Windows
`windows-native` CI job; no second harness or long campaign is required
to explain the cleanup operation.

Read-only `diagnostics view=operation` exposes a bounded account/operation
trace with operation identity, local BEGIN, attempted send, ACK/readback,
source timestamps, SQL receipt and specific blockers; the P95 <=100ms
warm-local test is a diagnostic target, not a venue SLO. This volatile
projection does NOT fabricate durable history after restart/eviction;
SQL remains the authority and an absent projection never proves no-send.

**Scope boundary:** #500 accepts only this operation and its directly
associated transitions; PR #499 remains Draft and **must not merge** until
full #449 CODE 26/26, S1/S2, required checks and branch integration evidence.
A documentation-only commit following the proven SHA does not change tested
Rust/JS behavior; it must not be represented as a new full-CODE PASS.

## #501 — unsent release: one production boundary

`trade_service/unsent_release.rs::release_unsent` owns the existing orchestration
for a durably rejected, never-sent intent. `TradeService::release_unsent` is a
thin adapter used by public Status, Execute rejection and maintenance through
Status. The former implementation in `reconcile.rs` is removed. Terminal
position release remains a different operation with its existing guards.

Dependencies are the account SQL handle, NativeRiskClient, governance lock and
per-call SQL deadline. The operation has no venue adapter. AccountStore still
owns no-send proof and acknowledgement; RiskBook still owns central money.
`UnsentReleaseStage` identifies `sql_proof`, `authority`, `inspect`, `release`
or `sql_ack` at the failing call, with the original reason code and known
intent/reservation identity. Public Status retains `central_release.reason_code`
and adds `central_release.failure`; Execute retains `risk_release_error` and
adds `risk_release_failure`. This is a result explanation, not a new persisted
event history or a claim that a pending operation is safe to resubmit.

```text
cargo test --locked -p okx-agent --lib trade_service::unsent_release::tests:: -- --nocapture
```

UR-01/02: missing durable proof/authority, no risk command.
UR-03: HELD → RELEASE_UNSENT → SQL ACK, repeat retains the first receipt.
UR-04: lost reply, actual SQL owner shutdown/reopen, INSPECT before acknowledgement.
UR-05: stale CAS reinspection and the unchanged four-attempt bound.
UR-06/07: denied, missing/invalid and unresolved replies; existing Posted/absent paths.
UR-08: real SQLite trigger rejects ACK; no false success, later readback recovers.
UR-09: local acknowledgement and Demo paths make no Worker call.
UR-10: two concurrent callers, ordered conflicting replies, one durable acknowledgement.

These tests use real SQLite and NativeRiskClient with scripted response facts.
They prove orchestration, not Worker economics, a real OS crash, or full CODE.
The first Windows run on `2c3f780c` passed **10/10 in 2.36s** after compilation
([CI 42519995459](https://github.com/iamaman11/okx/actions/runs/42519995459)).
The existing CI workflow supports `target_case=UnsentRelease` for these tests
without Workerd or the long trading campaign. The mandatory PR matrix and
full #449 release gates remain unchanged; this mode cannot certify 26/26.

## T10-11 — 90 connected crashes PASS, 2026-11-09

Code SHA `a8afc8dde375584e38e762a019bb1181f238be02`,
[Windows CI 42564173457](https://github.com/iamaman11/okx/actions/runs/42564173457),
[artifacts 13651265984](https://github.com/iamaman11/okx/actions/runs/42564173457/artifacts/13651265984),
[reviewed checkpoint](https://github.com/iamaman11/okx/issues/501#issuecomment-6732653618).

The targeted case passed in 273.158 seconds. Independently checked all 90
individual records: F0–F8 ten times each, 90 distinct original SQL roots,
actual OS kills, new recovery PIDs, original SQL and physical Worker readbacks.
Separate component kills do not contribute to 90.

F7 completes the missing boundary using production source capture. One signed
fill/bill is durable and pending, with zero lots. A task-local test barrier
pauses this capture before projection; no global scheduler or source padding.
The parent kills Native. A new AccountStoreHandle opens the original database
and invokes the production pending-fill projector before consumers reconnect:
one projection, then zero. One 0.1-contract lot, one fee entry, unchanged raw
facts and all financial totals: FEE=-0.0005 USDT, CAMPAIGN_LOSS_USD=0.0005.
Replay retains finance revision 2. No second Create; Worker remains POSTED
with the same budget after physical storage reopening.

The direct boundary uses explicit prepare/reserve/census inputs and recorded
venue responses. F7 does not certify automatic recovery scheduling, a fresh
NAV, public admission, central release or live trading. All nine phases retain
their individual scope documented below. This targeted PASS is not 26/26.

The first F7 run exposed a test observation bug: an unarmed ACK-loss flag was
incorrectly reported as consumed. RecordedAccountHttp now records consumption
at the actual loss boundary. Existing direct HTTP tests reject false loss
before send/while merely armed/after delivered ACK. The child wrapper also
retains bounded non-Rust exit codes and Node assertion locations; it no longer
replaces those with an uninformative missing-panic message. No production
money, risk limit or source budget changed. The case envelope is 420 seconds
for nine phases; production TTLs remain unchanged.

Next: required full26 PR CI and Rust/Clippy/architecture/Cloudflare/B1/S1/S2 on
one final candidate. No merge or whole-application acceptance yet. Historical
full23 and targeted PASS on other SHAs cannot be combined into full26.

## Historical T10-11 — 80 connected crashes, superseded by the checkpoint above

Code SHA `49a4b715aa4b9cfbbc7c6a7ee527ef9d6ca8762c`,
[Windows CI 42558849617](https://github.com/iamaman11/okx/actions/runs/42558849617),
[retained artifacts](https://github.com/iamaman11/okx/actions/runs/42558849617/artifacts/13648896803):
80 independent OS kills passed their assertions, ten per boundary:

- F0: before prepare invocation, no intent or reserve; reopening the original SQL
  and replaying prepare creates one identity. This is not an in-transaction kill.
- F1: PREPARED before reserve, expired command fixture; public Status persists
  COMMAND_EXPIRED without a reserve or venue POST. No production clock is changed.
- F2: Worker HELD committed, reply lost before local receipt.
- F3: local RESERVED committed, actual Worker INSPECT proves HELD, no BEGIN_POST.
- F4: actual Worker POST_AUTHORIZED committed, no local claim; replay of the old
  fence after restart is DENIED without changing the book sequence or budget.
- F5: Worker POST_AUTHORIZED and SQL POSTING claim committed before transport.
  Production account reopen changes POSTING to UNKNOWN; a second SQL claim is
  rejected and the intent remains in the pending index. Two public Status calls
  each consume an exact signed negative order lookup. Both remain UNKNOWN,
  with no terminal/no-send/release receipt and no POST. The original Worker
  remains POSTED, including after storage reopen: loss reserve 1.102 and entry
  reserve 3.0005 (including the pre-existing foreign 1 / 2) are retained.
- F8: Worker release committed, reply lost before local SQL acknowledgement;
  recovery reads RELEASED and writes one SQL receipt without a second mutation.

- F6: actual `submission::post_once` claims SQL, real TradeApi sends one recorded
  authenticated Create, ACK delivery is lost, then Native is OS-killed before
  the submission returns. The new process opens the original SQL as UNKNOWN.
  `observe_intent_order`, shared with production Status, independently finds
  order `123456` and records FILLED/TERMINAL. The actual source capture ingests
  one later fill and bill and projects one 0.1-contract lot, FEE=-0.0005 USDT
  and CAMPAIGN_LOSS_USD=0.0005. A second recovery/capture does not change those
  totals or resend. Both original and foreign Worker reserves remain held,
  including after physical Worker storage reopen; central release is absent.

Every new Native process opens its original account SQL. F0/F1 have no reserve
attempt, central receipt or release; actual Worker INSPECT remains NOT_FOUND and
its sequence/budget are unchanged. For F2/F3/F4/F8 public Cancel/Status persists
CENTRAL_RELEASE; replay retains the same receipt and reopening the same Worker
storage preserves RELEASED. F5 retains both candidate and foreign reservations;
none of its reads changes the Worker sequence or money. Seven phases have zero
venue POSTs; F6 has exactly one before the kill and zero during recovery.

Artifacts `T10-11/native-worker-fX-process-crashes.json` for X=0,1,2,3,4,5,6,8 and eighty
individual `native-worker-fX-process-crash-N.json` bind each repetition to its code SHA,
original SQL root, killed/recovery PIDs and SQL/Worker readbacks. Each repetition
creates fresh stores, then retains those stores across its restart. Native
crash executions never reuse memoized evidence. Series durations were F0=30.598s,
F1=30.051s, F2=30.411s, F3=31.316s, F4=31.845s, F5=30.746s, F6=52.196s and F8=30.817s.
The whole targeted case took 274.284s after compilation, including the retained
60 component kills (not counted as connected acceptance).
These measurements are not a guarantee
for a different runner. Recalculate bounded child/case/job envelopes when adding
the remaining phases; do not alter production source TTLs or trading deadlines.

This is a direct boundary test with explicit SQL prepare/reserve and census/causal fixture inputs,
not proof of full public Execute/admission, automatic recovery scheduling or a
live exchange. The separate component kills still lack connected Worker proofs;
some overlap the accepted boundaries. **Do not sum them into integrated acceptance.**
Connected F7 remains incomplete; T10-11 intentionally
ends `T10_11_ALL_NINE_NATIVE_WORKER_PHASES_TEN_TIMES_NOT_YET_PROVEN`.
Neither T10-11 nor 26/26 is accepted.

On this SHA, Windows compilation, formatting, direct release/recovery tests and
the new finite negative-order-source test passed before the crash series.
The negative source exception permits only code 51603 with empty data, exact
account, instrument and client ID. Positive recovery reads have literal venue
identities independent of the dead process ACK map; placeholder-based sources
still require their prior ACK binding. Foreign identities, unscoped/empty order
IDs and exhausted pages are rejected. The financial and single-send rules are
unchanged. Production submission and original-order observation were extracted
once and reused by Execute/Status and these direct tests; old inline paths were removed.

On the same `49a4b715` code SHA,
[CI 42559545446](https://github.com/iamaman11/okx/actions/runs/42559545446)
passed all ten direct unsent-release tests, the independent order/lost-ACK/original
order observation tests, formatting and Windows all-target Clippy with warnings denied.

Earlier component and regression evidence is explicitly SHA-scoped:
`1fb60885` passed [direct tests/Clippy](https://github.com/iamaman11/okx/actions/runs/42554147392)
and [T09.1](https://github.com/iamaman11/okx/actions/runs/42554511189).
`319de0b7` passed [direct tests/Clippy](https://github.com/iamaman11/okx/actions/runs/42555440202).
These runs cannot replace required gates on the final candidate.

F6 construction exposed two incorrect test inputs/expectations: a decision
without `manual` ownership conflicted with the plan/source fee owner, and a
FILLED order correctly produced a local TERMINAL receipt while central release
remained absent. The fixture now mirrors Execute's decision owner and the
direct observation test checks this terminal/release distinction. No financial
conflict check was weakened. On `301389a7`, F6 passed ten times, but a test-held
capture lease during shutdown added ten seconds per repetition and exhausted
the 300-second case budget. Releasing that lease after signaling shutdown and
letting the paused test consumer observe cancellation reduced F6 from 152.952s
to 52.196s. Neither the case timeout nor production deadlines were raised.

Next: F7 must kill after genuine raw fact/pending commit but before projection,
reopen the original SQL and Worker, run the existing projection/recovery, and
prove one fill/fee/lot/total through replay. Require ten independent F7 kills,
then all nine phases on one SHA. Account for the measured additional work in
the test envelope before the 90-kill run; never change source TTLs to fit tests.

Direct account-reopen regression, before the actual crash scenario:

```text
cargo test --locked -p okx-agent --lib accounts::tests::recovery_prepared_account_reopens_catalog_and_original_sql -- --exact --nocapture
node scripts/run-trade-integration.mjs --case T10-11 --artifacts <artifact-dir>
```

The first command checks original persisted SQL, catalog identity, duplicate
owner rejection and no POST. It waits for the existing SQL termination signal,
not an arbitrary delay. The second command remains a partial/FAIL acceptance
until every required F0–F8 boundary is proved ten times. A subsequent docs-only
commit does not represent a new tested runtime SHA.

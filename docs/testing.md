## #512 S0 — current production entrypoint map (working, not acceptance)

Source of truth: issue #512 r12, code of accepted main commit 9c36f50fc9a02d59d67de8e15b31950b1f58dea7 (tree 7805454a8abcf318ef4f0b1819b039aaf268435f), #510 exact FULL_PASS 61/61 properties and 85 witnesses in GitHub CI 43899933082. This section is navigation to actual Rust entrypoints, not an alternate business schema, ACL, or test oracle. Status: **S0 IN PROGRESS**, no #512 CODE acceptance claimed. Do not use historical checkpoints below as the current pass/fail state.

| Family | Supported operations / actual typed entry | Owner and source of truth / effect boundary | Direct verification / disposition |
|---|---|---|---|
| F01 CONTROL/Launcher | HostControlOperation::{Status, AccountCatalog, Sync, BuildAgent, DeployAgent, InstallLauncherRoot, UpgradeLauncherRoot, StageControllerUpdate, HandoffControllerUpdate, AbortControllerUpdate, ControllerUpdateStatus, WorkspaceStatus, TestWorkspace, InitAgentIdentity, AgentIdentity, BootstrapAgentGithubToken, ProvisionCloudflareRuntimeToken, StartAgent, StopAgent, RestartAgent, InitializeInstallation, InitializeAccount, BackupAccount, RestoreAccount, InstallAutostart, AutostartStatus, HandoffToAutostart, AcceptanceKillAgent, AcceptanceCrashController, AcceptanceFailNextControllerActivation, TransportStatus}. Control CLI::{Install, SetGithubToken, ActivateControllerUpdate, MigrateLauncherRoot, Once, Run}. | okx-protocol/src/lib.rs::HostControlOperation -> okx-host-control; CONTROL owns install/lifecycle and verified artifacts, not RiskBook/ledger; Agent CLI in main.rs owns separate offline SQL commands. | Existing host-control/launcher and #510 Windows physical/packaging tests; S8 SIMPLIFY verified Research component release only, all other commands KEEP. |
| F02 Identity/accounts | AgentOperation::ConnectAccount; Agent CLI::{InitKey, Identity, SetGithubToken, SetCloudflareToken, SetAccountCredentials, InitializeInstallation, InitializeAccount}; HostControlOperation::{AccountCatalog, InitAgentIdentity, AgentIdentity, InitializeInstallation, InitializeAccount}. | okx-agent/src/query.rs::execute -> accounts.rs::AccountRuntimeRegistry; storage_commands.rs for explicit stopped initialization; verified identity/catalog is authoritative, no silent reinitialize. | accounts::native_connect_contracts, storage_commands tests; S1 possible backend composition SIMPLIFY; identity commands KEEP. |
| F03 Governance/policy/CAS | Signed governance ingress and account policy / strategy capital revision. | cloudflare_transport -> trade_service/ risk_bridge -> native_finance_risk -> okx-execution unified owner; authenticated transport is not financial authority. | #510 policy/CAS, stale and foreign-scope witnesses; S1 REMOVE obsolete financial Cloudflare route; governance guards KEEP. |
| F04 Market/source/reference | AgentOperation::{Market, MarketCapabilities, MarketMicrostructure, FindInstruments, MarketOverview}; public/reference, OHLCV, funding, FX/fees/account source capture and NAV observations. | query.rs::execute/execute_market, market_bootstrap.rs, okx-runtime/okx-observation and account sources; data quality/freshness/version at source boundary; reads cannot grant risk. | market_bootstrap tests, query::public_dispatch_tests, recorded native trade source witnesses; S2 census proof SIMPLIFY; S6 public-source interface SIMPLIFY. |
| F05 Trading entry | AgentOperation::Trade with TradeRequest::{Capabilities, Preflight, Preview, Execute}; TradeOperation::{Open, Add, Reduce, Close, Replace} is a field in existing Execute (not extra public wire action). | query.rs::execute -> trade_service/mod.rs::trade_with_invocation -> execute.rs/plan.rs -> unified financial SQL owner -> submission::post_committed_native after durable claim; exchange adapter effects. | accounts::native_trade_contracts, execution native financial witness gate; S1/S2/S4 SIMPLIFY; no second POST owner. |
| F06 Trading correction | AgentOperation::Trade with TradeRequest::{Status, Cancel, Close}; Replace/Reduce/Close intents through supported Execute semantics; protection and emergency operations. | trade_service/reconcile.rs with protection_cleanup/protection_resize/terminal_release/unsent_release and account venue adapter; Status **may reconcile/mutate**, not a read-only diagnostic. | T09/T10-corrective, ACK/UNKNOWN/no-repeat, protection/late fill and #510 physical witnesses; S3/S4 SIMPLIFY; no new coordinator. |
| F07 Finance | Ledger, RiskBook, reservation, fills/bills, settlement, release, NAV facts, receipts; internal typed owner commands, not new public API variants. | okx-execution/src/account_worker/unified and risk/book/store; one finance.sqlite owner/transaction for ledger+RiskBook+receipt; account sources through same owner. | #510 financial/property/resource 61/61 gate and direct okx-execution owner tests; S1 REMOVE alternative writer construction; COMMIT/WAL/UNKNOWN invariants KEEP. |
| F08 Recovery/storage | Agent CLI::{BackupFinance, RestoreFinance, InspectFinance, BackupAccount, RestoreAccount, Once, Run}; HostControlOperation::{StopAgent, StartAgent, RestartAgent, BackupAccount, RestoreAccount}; startup/stopped finance and UNKNOWN recovery. | main.rs, runtime.rs, storage_commands.rs and unified owner; offline inspection/backup is not implicit migration, process exclusion enforced. | #510 startup/physical/OS-kill/reopen/backup tests; S1 rights SIMPLIFY, source/storage compatibility KEEP. |
| F09 Strategy | AgentOperation::Strategies with StrategyRequest::{List, CapitalAssign, Status, Create, Update, Start, Stop}; strategy_turn/decision claim. | query.rs -> trade_service/strategies.rs -> existing StrategyKernel + SQL claim; no research job executes or authorizes live decisions. | typed strategy/Native trade acceptance; S4 typed identities SIMPLIFY; S6 REMOVE live analytics service dependency while keeping kernel and guards. |
| F10 Research | AgentOperation::Research with ResearchRequest::{RecordDecision, Decision, Capabilities, Benchmark, Validate, Backtest, Grid, Status, Cancel, Result}; existing analytic market commands and jobs/cursors/exports. | query.rs -> analytics/mod.rs::AnalyticsService -> analytics/jobs.rs::Jobs (research-catalog.sqlite) / okx-research::ResearchArtifactStore; existing CPU executor and market source. Financial writer not accessible. | analytics unit/contract, K9 2-heavy test; S6–S8 MOVE implementation to one bounded child and replace in-process path; no fallback. |
| F11 Auth/transport | AgentRequest::validate, AgentOperation::action_class; CLI/Cloudflare MCP/WSS authenticated routing and session generation. | okx-protocol AgentOperation / account_governance -> okx-agent/query.rs::dispatch_with_governance; Cloudflare authorizes transport/session, does not commit finance. | query::public_dispatch_tests and actual workerd transport CI; S5 visibility KEEP/SIMPLIFY, S7 typed research IPC only. |
| F12 Read-only diagnostics | AgentOperation::Diagnostics with DiagnosticsView::{Finance, Operation, Research, Storage}; Portfolio/PortfolioScenario/AccountSummary and MailboxTelemetry remain separately typed product reads. | query.rs -> diagnostics.rs::diagnostics and finance SQL readback/projections; Status is **not** substituted for read-only explain; no POST or financial writes. | diagnostics unit/tests, #510 finance trace/recovery readback; S4 typed reason/evidence SIMPLIFY, research explain S7. |

**Initial finite changes/KEEP:** S1 execution_store.rs, risk_bridge.rs, account_worker.rs and their constructors/callers; S2 execute.rs/census and source proofs; S3 reconcile.rs and account/execute/analytics tests only as needed; S4 typed changed interfaces and diagnostics; S5 Rust visibility/architecture guard; S6 analytics/research live-source split; S7 research IPC and existing jobs owner; S8 component manifest/CONTROL and CI. All other production families remain KEEP unless their existing entrypoint is directly affected or a demonstrated financial regression blocks acceptance. Details of removed symbols must be bound to the actual patch, not claimed before changes.

**Known actual mixed boundaries:** Native startup main.rs requires finance.sqlite before creating AnalyticsService, and trade_service/strategies.rs::strategy_turn calls AnalyticsService::load_closed_closes. So S6 must decouple live closed-bar input and M01–M06 host availability; merely moving analytics to another process would regress trading. Current risk_bridge.rs also retains RiskRoute::Cloudflare; execution_store.rs retains Legacy/Native, but test-only uses and reachability must be distinguished before removal. Current host-control artifact::BundleManifest has one source identity for three EXEs; S8 must preserve component identity when adding independent research artifacts.

**S0 verification commands (planned, not executed here):** `python scripts/check_architecture.py`; `cargo fmt --all -- --check`; `cargo clippy --locked --workspace --all-targets -- -D warnings`; `cargo test --locked -p okx-agent --lib native_connect_contracts`; `cargo test --locked -p okx-agent --lib native_trade_contracts`; `cargo test --locked -p okx-agent --lib analytics::`; `cargo test --locked -p okx-execution --lib`. Current mandatory Windows CI workflow and exact property/physical gate remain authoritative; filtered tests never replace them.

**Performance measurement status:** #510 accepted CI reported 95.996s post-build runtime and 126.048s test build for its declared workload (not a new #512 baseline). S0's direct-test 5/10/30s and diagnostic p95/250ms targets remain *UNMEASURED / NOT YET FIXED* until three equivalent Windows runner runs with exact test IDs, seeds, fixture sizes, cache state and separate build/setup/execution times. No baseline or budget is invented from source inspection; S0 cannot be accepted on this note alone.

### #512 - removal of obsolete worker consumers (local verification)

The current test cut moves public Strategies and direct entry/unsent/terminal guards onto the actual Native fixture. The historical separate campaign SQL actor is deleted. Its SQL calculations remain component tests; one bounded FIFO and diagnostic lifecycle belong to `UnifiedFinanceHandle`.

| Removed responsibility | Current retained witness / disposition |
|---|---|
| Public strategy routing, CAS, capital, immutable old intent plan | `strategy_contract_tests` uses the Native fixture and typed owner methods. Historical definition-row retention remains in `account_store_tests::{immutable_strategy_versions_bind_old_intent_and_cas_start_stop,strategy_policy_retry_keeps_immutable_lineage_and_one_intent}`. |
| Entry settlement Worker ACK/CAS retries | Retired remote-money delivery; Native `native_partial_entry`, `native_fee_correction`, `native_post_release_correction` (through filled-close), reserve/post lost-reply and actual Create crash preserve current source/amount/revision/replay/no-second-send contracts. Direct Demo/missing-order/foreign-order guards remain. |
| Unsent Inspect retries, malformed remote response, second account ACK | Retired two-owner choreography. Native `native_reserve_recovery` and `native_changed_source` retain actual release, immutable receipt, same-key replay, cold reopen, foreign-plan refusal and no POST. Two direct authority/Demo guards remain. The targeted CI requires both economic witnesses in addition to these guards. No claim of an injected Native unsent post-write rollback. |
| Terminal stale fill/bill frontier and first-blocker diagnosis | Direct tests now perform real public Execute/lost ACK, signed zero-fill cancellation readback and the production release entrypoint. Actual financial rows remain unchanged. Native filled-close/post-release correction separately retain release/late revision/reopen behavior. Remote replacement-session ACK tests are removed. |
| Separate campaign/account worker queue, per-account SQL isolation | Current Native owner queue/backpressure/start-deadline/shutdown, scoped workload and diagnostic reopen tests. An account blocked on the shared writer cannot promise a second independent writer. No claim of interrupting an already-running Native SQL statement or an immutable per-call timing receipt; the current owner exposes aggregate counters. |
| Three decision origins reject changed policy before send | Direct SQL `three_decision_sources_sql_policy_change_fails_closed` retains each origin, replay, `POLICY_CHANGED` and absent POST fence. Native public/FIFO tests separately cover concurrency. |

Historical direct two-file source/settlement SQL tests remain explicitly historical components, not a second runtime and not a Native settlement-COMMIT OS-kill witness. F3/F5 lost replies/clean reopen, F6 Create OS kill, and a kill immediately after Native settlement COMMIT are different claims. Do not merge their results into an unexecuted scenario.

Local Windows debug verification of this cut: Agent 444 PASS / 4 ignored / K9 separately filtered, 49.75s; Execution 339 PASS / 2 ignored, 12.33s. The first Agent run was 441 PASS / 3 FAIL: the new read-only financial-row oracle used the wrong database path. Correcting it to finance/finance.sqlite produced direct terminal 5/5 PASS (6.45s) and a subsequent full Agent PASS. No trading rule or financial assertion was relaxed. These workloads differ from prior test counts and are not a full property/Windows CI performance comparison.

Remaining S1 dependencies include the raw historical transport campaign, old K5 fixture, account-worker actor/feature, registry constructor and ExecutionStore fallback branches. Actual workerd retirement tests use a JavaScript peer; they do not by themselves prove Rust `run_connected_session`. Before removing the raw campaign, retain its unique production Rust session/governance/observation properties with direct current-owner tests. No new accepted SHA_A or complete #512 result is implied by this source map.

### #512 — coordinated Native fixture and command ownership slice

Base: PR #513 `c25340d`. Cancel/Replace/Close now live in `trade_service/reconcile/{cancel,replace,close}.rs`; Status and corrective protection in `reconcile/status.rs`; shared evidence guards stay in `reconcile.rs`. Existing status test namespace is retained through `reconcile/status_tests.rs`. Command bodies were compared to the base after visibility/format normalization.

Current-owner replacements:
- Source frontier and cold diagnostic restore: `accounts::native_trade_contracts::native_source_diagnostic::{campaign_readiness_requires_capture_of_new_source_frontier,recovery_prepared_account_reopens_catalog_and_original_sql}`. The latter replaces the old `accounts::tests` exact CI name; zero execution still fails.
- PC01–PC07: same direct-test names, current `UnifiedFinanceHandle` and scoped operations; PC05 reopens the same financial database. PC07 obtains intent/protection through public Execute, signed fill capture and Status before its direct cleanup/concurrent-call checks. Its independent flat observation is a component input, not a full public Close lifecycle proof.
- F1: `account_worker::unified::owner_admission_crash_tests::risk_book_contract_native_os_kill_prepared_before_reserve_rejects_unsent` kills after actual prepare and before the reserve-attempt marker, then verifies durable absence of risk/POST and idempotent rejection.
- F7: `accounts::native_trade_contracts::native_crash::risk_book_contract_native_os_kill_pending_fill_projects_once` kills after signed fill ingestion, before projection; original fee and one lot survive exact replay. This is not a kill inside projection COMMIT.
- Research/public dispatch and trace/source contention use current Native fixtures. The Native storage metadata view explicitly remains NOT_EVALUATED/null with NATIVE_STORAGE_PROJECTION_NOT_AVAILABLE; it must not invent the retired per-account cached policy. Occupied FIFO and known durable receipt recovery remain separately proved by existing Native diagnostic contracts.

Local Windows debug verification: Agent 462 PASS / 4 historical ignored (K9 separate), K9 1 PASS with 30 measurements, Execution 350 PASS / 2 ignored, Protocol 41 PASS / 1 ignored; compile-negative docs Execution 1 and Agent 2 PASS. Agent runtime 164.01s, Execution 22.87s, K9 3.72s; not the full 61-property runner or GitHub Windows gate. The first broad run failed 3 file-byte checks on active Native lock files. A shared test inventory now retains coordination paths without reading their lock bytes; DB/WAL/data bytes and new files remain checked, with a negative control. Focused fix 5/5 and subsequent broad Agent run passed. Strict multi-package all-target Clippy passed; standalone default Execution doc build still reports legacy-only unused helpers, to remove with S1. This is not an accepted SHA_A.

Historical K5 phase repetition and Worker F7 are not replaced wholesale by these two new crash points. Legacy worker/feature/branches still exist; S1, SHA_A, SHA_B and full #512 acceptance remain incomplete. Actual executed results belong to their exact source and report, not this navigation map. No installation or live trading is accepted by this slice.

### #512 — Scope and stale-generation properties on current owner

The old `wrong_uid_rejected_before_post` and `view_selection_does_not_reroute_intent`
test functions were retired. They used a legacy per-account SQL handle
and are replaced by the actual Native wrong-UID bootstrap, account-scoped
diagnostic and two-account isolation contracts:
`risk_book_contract_native_connect_wrong_uid_does_not_create_account_or_publish_catalog`,
`risk_book_contract_native_intent_diagnostic_is_account_scoped_and_refuses_foreign_installation`,
`risk_book_contract_native_two_accounts_share_budget_and_isolate_public_capital_receipts`.
The old `stale_generation_response_ignored` simulation was replaced by a
direct pure test of the same production `validate_generation` guard, paired
with `risk_book_contract_native_changed_post_held_fee_releases_unsent_and_replans`
for the real source-change/unsent behavior.

The `prepare_on_owner` test helper **must remain temporarily** because it
is still invoked by the larger `#[cfg(test)]` legacy Execute preparation
branch. That branch and its original tests must be migrated together, not
silently removed; it is **not** a production financial write path.

The acceptance evidence consumer `scripts/acceptance_product.py` has
been updated too: `EXEC-01` and `MULTI-01` now require executed, fully
qualified **real Native owner test names**, never removed legacy tests.
The fake-venue action guard accepts only current Native business tests plus
its existing exact Worker route witness; it still rejects any supposed
physical Real exchange mutation proof. The script's own `--self-test` is
required in Draft CI. Missing Native tests remain missing evidence, not PASS.

### #512 — Native account bootstrap supersedes per-account worker fixtures

Two old tests were retired from `accounts.rs` because they opened
independent legacy `account.sqlite` writers; current installed Engine never
does that:

| Removed test | Current-owner equivalent |
|---|---|
| `prepared_account_rejects_unverified_identity_before_sql` | `risk_book_contract_native_connect_wrong_uid_does_not_create_account_or_publish_catalog` — original native signed-UID denial cannot create an account or publish catalog state |
| `recorded_two_account_bootstrap_opens_independent_sql_owners` | `risk_book_contract_native_connect_two_accounts_and_reopen_one_original_sqlite` + `risk_book_contract_native_two_accounts_concurrent_owner_reserves_share_entry_cap` — verified two-account isolation and reopen on one original `finance.sqlite` owner, not two retired writers |

The `accounts.rs` tests for original source-frontier readiness,
recorded multi-page capture and historical diagnostic recovery **remain**
until equivalent direct Native properties are demonstrated. This does not
remove the test-only registry fallback or justify S1 acceptance.

The full Windows CI exact-name recovery gate must not silently invoke a
removed legacy fixture. It now requires **two** real current-owner witnesses,
`accounts::native_trade_contracts::risk_book_contract_native_public_execute_lost_ack_and_replay_send_once`
and `accounts::native_trade_contracts::native_crash::risk_book_contract_native_os_kill_after_create_recovers_original_unknown`.
Both must report exactly one executed PASS, including a cold OS-kill/reopen
boundary; a filtered-out test is a failure, not a passing substitution.

### #512 — Native POST/replay/UNKNOWN witnesses replace retired worker tests

Eight old per-account `AccountStoreHandle` send/recovery simulations were removed
from `trade_service/execute.rs`, with their actual properties retained by
tests using the real `UnifiedFinanceHandle` and public Agent entrypoints:

| Removed legacy test | Current owner test(s) proving the retained property |
|---|---|
| `prepost_fence_denial_is_durable_no_send_not_unknown`, `stop_after_reserve_before_post` | `risk_book_contract_native_execute_bound_rejection_preserves_code_and_reserve`; `risk_book_contract_native_public_cancel_after_held_replays_and_cold_reopens_without_send` |
| `lost_http_ack_reopens_original_sql_without_second_send`, `lost_response_after_post_returns_unknown` | `risk_book_contract_native_public_execute_lost_ack_and_replay_send_once`; `risk_book_contract_native_os_kill_after_create_recovers_original_unknown` |
| `k5_f5_f6_reopen_sql_never_reposts_unknown_or_invents_no_send`, `restart_posting_reconciles_without_resubmit` | `risk_book_contract_native_os_kill_after_create_recovers_original_unknown`: genuine OS death, durable UNKNOWN, no second POST |
| `duplicate_execute_one_post` | `risk_book_contract_native_execute_100_callers_one_send_policy_cas_and_payload_conflict` |
| `two_accounts_two_instances` | `risk_book_contract_native_two_accounts_share_budget_and_isolate_public_capital_receipts`; `risk_book_contract_native_two_accounts_concurrent_owner_reserves_share_entry_cap` |

The Native lost-ACK test also verifies the persisted semantic `request_hash`
against the original canonical request on the actual financial owner before
replay. The **old 60-kill Windows K5 fixture remains temporarily** because
its repeated/all-phase coverage has not been replaced by an equivalent Native
Windows matrix. It must be migrated before the legacy worker can be deleted;
passing historical fixture tests is not acceptance of the current owner.

### #512 S1 — migrated old-worker property (same current-owner code path)

| Previous fixture property | Actual current-owner witness | Disposition |
|---|---|---|
| `account_worker::diagnostic_fault_tests::failed_accepted_sql_job_still_invokes_timing_observer` | `unified::owner_queue_tests::risk_book_contract_fresh_owner_failed_job_counts_and_preserves_installation`: failed accepted SQL job increments the ONE finance owner job counter exactly once, preserves the committed installation receipt and allows safe reopen | Replaced; the retired per-account timing callback is not an acceptance interface |
| `account_worker::diagnostic_fault_tests::real_account_worker_holds_shared_installation_mode_until_close` | `unified::owner_startup_tests::risk_book_contract_new_owner_exact_replay_reopen_and_no_trade_permission` + `finance_owner_gate::tests::legacy_stopped_maintenance_uses_same_cross_process_mode_and_rejects_import` | Removed obsolete writer-lifecycle test; original exclusion property preserved by current writer/exclusion tests |
| Old worker backup cadence, maintenance projection and shutdown-reader tests | Unified owner FIFO/shutdown/reopen, native SQL diagnostics, stopped whole-finance snapshot and real crash tests | Old worker mechanisms are not a required live owner contract; verify current witnesses before deleting fixture |

Do **not** count any `legacy-account-worker-fixtures` test as proof of shipping finance behavior. The fixture feature is temporary, its deletion and full current-owner replacement are required to accept S1. Production build uses no such feature. The S1 current-owner direct properties above still require exact-SHA CI verification.

## Current-architecture CI layout and asynchronous research contract

CI43887700047 at c8999955 passed architecture, Linux Rust/core, actual workerd
transport and the current B1 replacement. Its flat Windows regression failed
one current Native positive test with safe COHORT_ACCOUNT_SOURCE_STALE/no send
and two research tests which treated a valid queued job as a completed table.
Those failures were not waived as obsolete. Local FULL_PASS did not replace CI.

Research fixture tests now accept the actual bounded market contract: a ready
page OR the original job identity, then durable terminal state and the original
cached artifact. The publication test deterministically holds the real CPU
permits until market returns its legitimate pending response, then releases
workers and verifies the actual result/replay. No synthetic job, longer public
timeout, new service, changed financial formula, source time, freshness/risk
rule, extra GET page or POST retry is introduced.11 artifact cases passed in
2.27s; strict Clippy passed.

The Windows job now executes the exact current Rust financial/physical/resource
gate FIRST, uploads that evidence when the step ran, then retains ALL native
regressions partitioned by domain. Trading/SQL/CONTROL/Launcher run separately
from CPU-heavy analytics; each broad libtest group has2 fixture workers. This
is host-load isolation between independent test installations, not removal of
concurrency inside 100-caller/shared-budget/fairness tests. K9 still runs its
actual2-heavy-job/ingestion/Stop/cancel workload with30 measurements separately;
it is skipped only in the general analytics group to avoid measuring it twice.

Local exact regression commands:389 Agent trading +CLI6 +execution346 +CONTROL46
+Launcher19 PASS;81 analytics PASS, K9 separately1 PASS. Historical ignored
probes remain ignored and provide no current property credit. Agent totals
389+81+K9=471, preserving the complete prior library coverage. Entry-settlement,
protection-cleanup and unsent-release tests remain in that full library run;
their duplicate filtered invocations were removed from the same CI job.
The single current financial gate still requires61/61 atomic rows and fresh
physical/source witnesses. Historical26 driver results are never reused.
## Current CODE checkpoint and replacement of the remaining B1 driver

The full Windows collector at c89ecee8 passed61/61 atomic rows with84 core
witnesses plus the measured-budget witness in82.759s after build. The genuine
64-COMMIT/16-concurrent/reopen/replay workload passed in0.662s with128 legs,
p50 47.572ms/p95 107.337ms/max122.831ms, queue116ms/SQL19ms. Declared direct
CPU/RAM/IO, storage and same-workload regression budgets passed. These are CODE
properties, not installed/physical-host/live-market acceptance. Historical
checkpoint sections below remain historical; authoritative current status is
GitHub #510 and the executed manifest/report, not old NOT_PROVEN prose.

A full workflow review then found an obsolete independent B1 job still calling
the removed distributed Native/Worker financial driver. That job is replaced,
under the same check name, by FIVE exact current public Rust witnesses: shared
A/B denial preserving A's send, positive B while A source is blocked, B Status/
Cancel independence,100-callers replay/conflict/policy-CAS, and strategy CRUD/
claim/CAS. TWO current single-writer queue tests replace the separate-owner
queue prerequisite. Counts must be5/2; absent/zero tests fail. Retained actual
workerd auth/session/generation/dispatch/denial and genuine child kill/reopen
remain in the mandatory full gate. Remote financial CAS/old WSS money ACK is
retired because that authority no longer exists, not because a test failed.

Only workflow/docs changed after c89ecee8; rerun the full current collector and
all required GitHub jobs on the final committed SHA. The prematurely launched
old-workflow CI was cancelled to avoid spending time on its obsolete driver.
No merge, deployment, install, credential or financial-history change occurred.
## RB16 final acceptance envelope (declared before final run)

The current acceptance workload is the exact reviewed Rust witness set (84
core witnesses at the d35565e0 baseline), including public A/B trading, Add,
partial/late fills, closes, UNKNOWN, source/refusal/diagnostic/recovery paths,
real child kill/reopen/exclusion and retained workerd transport. The64-transfer
workload adds16-concurrent SQL pressure,64 exact replays,65 original owner jobs
and128 conserved ledger legs. This is a finite CODE workload, not proof that
all32 possible accounts or unlimited markets sustain a throughput rate.

budgets.rs enforces, without modifying production limits:
- Whole setup/physical/teardown:180s; build and metadata collection separate.
- Sum of direct measured process CPU:90s; observed peak of each direct process:
  512MiB; sum of direct Windows process read bytes and write bytes:512MiB each.
  These include pipe I/O. Descendants are excluded; this is explicitly NOT an
  aggregate OS-process-tree resource or power-loss guarantee.
- Workload:5s for each command and observed queue/SQL maximum;30s complete
  setup/reopen/replay/teardown;32MiB isolated financial storage.
- A missing counter, missing/duplicate/foreign stage, wrong run, wrong operation
  count, malformed latency vector, false percentile or exceeded budget cannot
  satisfy the measurement witness. Full mode refuses unsupported counters.

The Windows x86_64 debug/threads4 baseline at d35565e0 is89.091/81.798/82.765s,
8 logical processors, Intel64 Family6 Model60 Stepping3. Selected core witness
hash is076f617c6aaf220a3b4a2506a28a05ce1d17d020363b68f46541d09610af6467.
On that hardware class and identical witness set, the ceiling is111.364s
(25% above observed maximum); a changed machine/workload is explicitly not a
comparable speedup/regression claim. Portable180s gives about2x baseline max;
CPU90s gives over2x observed direct CPU;512MiB gives over3x observed direct
peak and about3x aggregate reads/writes. This is an initial regression envelope,
not an optimization result. #512 owns subsequent measured reductions.

p50/p95/max are empirical nearest-rank values over the64 real command latencies,
including queueing. Samples are correlated within16-way batches, so no population
tail or independence is claimed. One full-run duration is reported separately,
never as whole-run p95. An optional repeated collection still requires20samples
to emit whole-run p95; repeating the entire suite20times is NOT required for
initial CODE acceptance. #510 explicitly records this clarification. Financial
witnesses still run fresh once per full invocation; no prior PASS is reused.

Both RB16 manifest rows now require an EXECUTED measurement witness, not just
the presence of constants. Windows full mode can emit FULL_PASS only after all
current properties, workload export/readback/cleanup, counters and budgets pass.
Linux known mode retains explicit MISSING measurement rows when counters are
unavailable; unavailable counters are null, never synthetic zero measurements.
Final local/GitHub outcomes must be recorded against the committed candidate.
## Bounded workload and measured acceptance stages - 16 November 2026

The fresh installation writer now has a declared financial workload: four
batches of sixteen concurrent typed capital assignments (64 genuine COMMITs),
then shutdown/reopen and all64 exact receipt replays. Independent assertions
require128 balanced ledger legs, assigned0.64 from equity100, residual99.36,
no new legs on replay, exactly65 observed jobs before shutdown, per-operation
and max queue/owner5s watchdogs, complete setup/reopen/cleanup30s watchdog,
and at most32MiB in the isolated finance directory. These debug CODE budgets
are not an exchange throughput SLA, census32 stress test or power-loss proof.
The first direct execution passed in1.11s; the exact candidate collector must
execute it again. The mandatory RB16 row remains MISSING until the complete
public workload and resource budgets are established.

The existing Rust collector records wall time per package, static guard and
retained real workerd transport. On Windows it duplicates each direct child's
kernel handle, records process CPU100ns and I/O counters, samples the observed
peak working set every20ms, joins the sampler and closes the owned handle.
Descendants are explicitly excluded; Windows I/O includes pipes, not just disk.
Unavailable or incomplete counters are explicit, never converted into zero.
This does not claim complete crash-child/workerd-tree resource accounting.

`cargo run --locked -p okx-agent --example native_finance_acceptance -- --known --samples 20`
executes twenty fresh collections on the same clean HEAD/tree. Build time is
reported separately for every sample; setup, child exit/readers and retained
physical/transport checks stay inside runtime. Any witness failure stops the
run. p50 requires3samples; empirical nearest-rank p95 requires20samples and
makes no population-tail guarantee. One run yields no percentile. Old binaries
cannot certify modified collector sources, including the new resources module.
The known subset remains PARTIAL and default full mode remains BLOCKED while
mandatory resource/budget rows are missing. No new runtime financial component,
release installation, credential access or trading action is introduced.
## Production boundary cleanup and strict verification - 16 November 2026

Strict Clippy with -D warnings now passes for Agent, CONTROL, Launcher and
execution, all targets. Full library regression: Agent471 PASS/4 historical
ignored/45.01s; execution345 PASS/2 ignored/16.00s; kernel25 PASS; protocol41
PASS/1 ignored; runtime21 PASS. Agent CLI startup6 PASS/0.08s; CONTROL46 PASS/
8.25s; Launcher19 PASS/10.71s. Zero-test binary/doctest targets add no evidence.
Ignored historical cases never satisfy mandatory current properties.

The old standalone campaign worker/Connection, mutating offline import helpers
and unbound fixture-only command variants compile only under cfg(test). Current
source-bound Reserve/BeginPost/settlement/release continue using the one live
installation transaction. Read-only legacy inventory inspection is unchanged.
SQL decoder rows have local names; automatic style fixes preserve predicates,
column order and amounts. No allow(dead_code), weaker schema, reset/import,
changed source time, money limit or lost-ACK rule was introduced. Narrow expect
annotations document existing eight-argument SQL/receipt boundaries and keeping
the bounded232-byte receipt inline; they are style decisions, not safety waivers.

The Windows CI no longer gates on the retired JavaScript Native/Worker finance
SCENARIO DRIVER's8/5 lifecycle tests. Those test driver metadata/PID handoff,
not the current shipped Engine. Current Rust child/SQLite/real process and
Engine drain witnesses plus real workerd auth/session/generation/typed-denial
remain in the mandatory fresh gate. CONTROL/Launcher tests and #449 installed/
physical lifecycle acceptance remain mandatory. No old driver result is reused.
Collector negative controls now execute explicitly in the full Windows job.

RB16 workload/resource/percentile evidence is still NOT_PROVEN. Neither strict
Clippy nor the903 passing library tests can close those rows or the full CODE
acceptance. The current collector must be rerun on the committed cleanup SHA.

## Terminal census config causality correction

GitHub run43805408497 on4ea11685 failed the emergency terminal release with
TERMINAL_CENSUS_PRECEDES_SETTLEMENT_SOURCE. The ordinary observer snapshot
retained the authentic timestamp of a still-valid cached config, which could
predate settlement even though balances were read later. Terminal census now
uses the EXISTING forced authenticated config ingress; no source time is
relabelled and no freshness/census/release guard is relaxed. A deterministic
cache-frontier unit witness distinguishes the old cached time from the new
response. The four terminal/Close lifecycle cases plus existing forced-config
negative contract passed locally5/5 in18.54s. A new full current-SHA collector
and GitHub validation remain required; the previous59/61 report is Windows
partial evidence, not cross-platform GO.

## Native completion boundaries - 16 November 2026, current working slice

The current collector maps 61 atomic properties. Its only intentionally missing
rows are RB16 workload/resource budgets and comparable complete measurements.
Missing stays BLOCKED in full mode; --known reports PARTIAL, never FULL_PASS.
Do not infer acceptance from the number of tests or old distributed26 results.

Additional current production witnesses:
- A second DOGE linear-USDT contract uses its actual multiplier/tick/lot/minimum
  and explicit permission. Invalid grid/subminimum/permission grant no intent
  or money. BTC defaults remain unchanged; quote input uses the original parser.
- Signed-source settlement of A consumes the shared budget before B admission;
  B sees remaining0.4995 and cannot reserve another1.0005. Its complete census
  includes the original A lot and independently observed live stop.
- Account STOP A and release/cancel A preserve B's original UNKNOWN receipt.
  B Status and Cancel also finish while A's separate HTTP read remains blocked.
- Identical venue fill/bill IDs across A/B pass through the actual scoped source
  ingestion APIs, deduplicate independently and preserve exact fees on reopen.
- Successful terminal Resolve is staged with both ledger and RiskBook ACK,
  rolled back with no residue, then committed/reopened/replayed exactly once.
- The existing full public lifecycle now receives an actual late -1 bill AFTER
  verified release: immutable correction, exact debit/breach, no refunded entry
  budget, exact replay and +1 rebate; original release receipt survives reopen.
- An unavailable safety-cancel ACK keeps its durable POSTING marker; replay
  cannot send another cancel or release exposure. A separate failed-stop path
  performs the original public emergency Close and only clears its incident
  after independently proved flat/finance/release/orphan-free remediation.
- Native intent diagnostics bound ONLY the read-only snapshot wait; a busy owner
  returns explicit incompleteness, not NOT_FOUND. Busy/reopen and stale-tariff
  drills read original identity/receipt/reason without network or permission.
- Shipped Engine task composition now accepts an injected shutdown notification
  for direct CODE testing; public run_engine retains the original Ctrl-C/stdin
  path. Actual maintenance installs opening and drain retains the same owner.
  This is not installed CONTROL or physical stdin/WSS acceptance.

Rust collector source bytes are bound to its build as well as current clean
HEAD/tree: a saved old collector cannot certify a new manifest. Exact current
artifacts/results and negative controls remain required. Retained workerd
transport/auth/session/typed-financial-denial verification runs inside the same
fresh collection. Removed Cloudflare finance-owner OS-kill repetitions are
RETIRED: workerd dispose is not claimed as OS death or money durability.
Native real OS-kill/SQLite/reopen witnesses retain their original guarantees.

## Integrated completion checkpoint - 16 November 2026

Current Native regression: 35 passed, 0 failed (56.87s after build).
Rust components: execution 342 passed / 2 ignored (11.91s), kernel 25 passed,
protocol 41 passed / 1 ignored, runtime 21 passed. Acceptance collector negative
controls: 6 passed. Ignored tests are not acceptance evidence.

Completed production paths in this slice:
- Source change after HELD: one SQL transaction rejects a proven unsent intent
  and releases its original reservation; a fresh plan uses the changed fee.
  A committed BeginPost cannot use this path, including lost COMMIT replies.
- Public Add retains the original owned lot and protects only the increment;
  external venue exposure refuses admission. B can execute while A's fee read
  is blocked; canceling A releases only A, preserving B's UNKNOWN obligation.
- Reserve and BeginPost COMMIT/reply loss use the actual owner transaction,
  exact readback, reopen and original receipt; no second send claim is issued.
- Fresh instrument metadata comes from bounded real REST reads only when new
  risk needs it. Generation/revision CAS rejects intervening WS changes;
  unchanged metadata preserves book state. Reductions keep their existing path.
- Windows catalog publication retains DELETE-capable staging/target guards;
  a real incompatible reader refuses publication without changing SQL identity.
- Read-only diagnostics include durable BeginPost, original receipt state and
  SQL no-send proof; they never send, release or manufacture current venue truth.

One Rust collector now maps RB01-RB16 to exact production witnesses, executes
current Cargo artifacts, binds HEAD/tree, rejects ignored/missing/duplicate or
foreign results, and separates build from setup-through-teardown time. Run:
`cargo run --locked -p okx-agent --example native_finance_acceptance -- --known`
for partial evidence. Default full mode MUST exit BLOCKED while required rows
remain MISSING. The manifest is the explicit executable gap list, not a claim
that #510 is finished or that full acceptance takes seconds. The old distributed
financial matrix is not a substitute for this current architecture's contracts.

This section supersedes older statements below that Add, unsent convergence,
COMMIT-reply loss or the reproduced catalog publication boundary lack witnesses.
Full RB acceptance, resource budgets and shipped startup composition remain open.

# Current #510 integrated Native checkpoint â 2026-11-16

This checkpoint is NO-GO for complete RB01âRB16 / historical26 acceptance.
All results below are fresh LOCAL executions on this code; build is excluded.

- Native public composition:25 PASS,0 FAIL,0 ignored,28.16s.
- Execution:340 PASS,0 FAIL,2 historical ignored,12.23s; kernel25 PASS,0.01s.
- Storage commands:10 PASS,0.91s (including whole-finance wrapper).
- Existing adjacent contracts PASS: connect5,coverage verifier4,diagnostics29,
  plan13,terminal release8,unsent release10,protection cleanup7,entry settlement8.
- All-target Agent type check, format, architecture/self-test and SQL guard PASS.
- These are separately measured targets, not one full run, p95 or a latency SLA.

Completed boundaries: public partial Close0.04 then Close0.06 with independent
fees; explicit shared-budget Reserve denial B cannot strand A; caller canceled
after HELD retains original reservation on same-key replay and cold reopen;
changed post-HELD fee refuses send without fabricating unsent proof; whole-DB
WAL-aware backup and missing-target restore preserve identity and budgets and
require the original storage audit. Native operation diagnostics now read the
original receipt and account source generation/watermark in one scoped SQL
snapshot, without network, renewed freshness or a send capability.

Causal REDs: partial terminal Close incorrectly required filled==planned;
explicit Native Reserve Denied left an unresolved attempt that blocked A.
Snapshot contract setup originally failed CAMPAIGN_READINESS_STALE after
reconnect; it now uses the existing production maintenance refresh over actual
captured rows before testing the separate STORAGE_RECOVERY_INCOMPLETE fence.
No production freshness, source coverage or risk-release rule was relaxed.

Detailed production symbols and negative boundaries are in
cloudflare/okx-mcp/FINANCIAL_WITNESS_RETIREMENT.md. CI includes the new Native
backup wrapper. A Draft job success with other jobs skipped is not full GO.

Still missing: complete executable atomic-property map/collector and measured
workload budgets; public Add/second spec/fresh replan; positive B trading
fairness and independent obligations; Reserve/BeginPost-specific committed
reply-loss/remaining transport witnesses; all diagnostic drills and emergency
variants. The earlier intermittent Windows account-catalog publication error
is not declared fixed by later PASS. CODE does not imply installed/real trading.

The material below records earlier checkpoints and historical requirements.

## #510 r7 - Parallel financial safety checkpoint (Draft / NO-GO)

Issue #510 r7 is the single current plan; prior sections below are historical.
One Rust owner, one financial SQLite, no Cloudflare financial grant. Reuse
existing production tests; unsupported markets and unknown state fail closed.

Current complete Native trade target:20 PASS/0FAIL/0ignored,21.88s after build.
Complete execution library:337 PASS/0FAIL/2 historical ignored probes,10.53s.
Separate adjacent groups also PASS: coverage verifier4, native connect5,
plan13, diagnostics29, storage7, native risk1, native admission2, capture8.
No full26/RB01-RB16 gate or combined runtime/p95 claim is made by those totals.

New exact witnesses and their limits:
- accounts_native_partial_protection_contract_tests: actual partial stop,
  failure+late fill durable fence; partial-reduce stop resize requires independent
  exact GET, ACK alone cannot verify; no second safety/amend POST.
- accounts_native_fee_correction_contract_tests: late fee, independent bill,
  immutable revisions and replay; real rebate does not refund breach/capacity.
- accounts_native_multiaccount_contract_tests: verified initial A/B roster,
  same-key public capital preparation isolation and B progress after A source loss.
- accounts_native_multiaccount_reserve_contract_tests: concurrent original
  typed owner reserves share1.5 cap (one1.0005 Held, one denied); exact scoped
  replay and retained obligation. It is not full public A/B Execute/census.
- owner_financial_reply_tests: accepted financial transfer still commits after
  caller cancellation; commit/reply loss readback preserves all source/ledger legs.
- owner_financial_crash_tests: REAL child death before/after financial COMMIT,
  same SQLite/WAL reopen, zero partial legs, exact replay, second-writer refusal.
  This transfer proof does not replace venue/Reserve-specific fault evidence.
- diagnostics/native_intent.rs: production operation reference intent:<id>,
  durable scoped SQL snapshot, explicit unknown evidence, no HTTP/POST. The
  original actual venue-kill test also checks this diagnostic after reconnect.
- native_acceptance_coverage_tests: negative checks for a declared result set;
  full runtime collector and complete property-to-witness manifest still missing.

A real defect was reproduced by independent late-fee data: lazy guard DDL changed
canonical Native schema and caused FINANCE_INSTALLATION_SCHEMA_MISMATCH on the
next public command. Schema v8 creates all four fee/late immutable guards at
bootstrap. Native fee/late settlement is DML-only; original guard constraints and
freshness are preserved. Unknown/prior v7 refuses without automatic upgrade or
reset. Existing installed C:\okx was not read or modified.

Planner waits on existing versioned market notifications before rechecking the
predicate, replacing50ms polling without changing market freshness/deadline.

Current scope is OKX linear USDT swaps; census max32 accounts and owner queue128.
This is an extensible bounded application, not evidence of arbitrary markets,
instruments or unbounded accounts/trades. Full current acceptance, remaining
public Add/source/multi-close/post-release correction paths, financial fairness,
Native backup/restore and installed lifecycle remain open. Earlier intermittent
census/child failure is not declared fixed by subsequent successful repeats.

---
## #510 r6.1 - Public Execute safety checkpoint (Draft / NO-GO)

Current work order: issue #510 sections E-G. #512 is subsequent measured
optimization. Replacing the retired financial driver belongs to #510; an
arbitrary <=10s target no longer applies. Build time is separate from the
complete test wall time, including setup, physical tests and teardown.

Reuse existing direct production contracts rather than rewrite 26 campaigns.
The existing `native_trade_contracts` CI filter now includes:

- `native_execute_safety::risk_book_contract_native_execute_bound_rejection_preserves_code_and_reserve`:
  exact bound venue rejection, actual order code 51008, durable REJECTED, one
  Create, no release inferred solely from ACK.
- `native_execute_safety::risk_book_contract_native_execute_all_failed_envelope_preserves_bound_rejection`:
  envelope code 1 with exact bound sCode 51008 is rejection, never acceptance;
  contradictory/foreign ACKs remain UNKNOWN (also covered by the ACK unit test).
- `native_execute_safety::risk_book_contract_native_execute_foreign_rejection_remains_unknown`:
  a raw foreign ACK reaches the production verifier, retains UNKNOWN and the
  reservation. The transcript guard is not used as a false ACK-rejection oracle.
- `native_execute_safety::risk_book_contract_native_execute_100_callers_one_send_policy_cas_and_payload_conflict`:
  100 cold public callers, a barrier before preparation, 99 local no-send
  denials, one Create/lost ACK and one original UNKNOWN identity. A later policy
  cannot rewrite that intent; stale/conflicting policies and an altered payload
  fail without changing the held money. This is not 100 concurrent Status calls.

Current local tree: 13/13 Native trade PASS in 15.09s after build. Three bounded
fresh re-executions of the same binary also PASS: 15.31s / 15.44s / 15.16s.
Complete execution library: 332 PASS / 0 FAIL / 2 historical ignored probes,
10.89s, including the new direct temporal-boundary diagnostic test. These are
separate targets, not the complete 26-property gate or a p95 claim.

An earlier run had two intermittent failures: child exited before Create and
STOP/Close reported COMPLETE_ACCOUNT_CENSUS_UNPROVEN. Their original cause is
NOT claimed fixed merely because subsequent runs passed. Child stderr/stdout
failure evidence is now retained; terminal census distinguishes exact temporal
boundaries without relaxing freshness, causality or release requirements.
Track repeatability/source ordering as an outstanding full-gate concern.

Multi-account/partial-protection/correction obligations, the complete-gate
self-test and installed CONTROL/WSS lifecycle remain NOT_PROVEN. Full criteria
are #510 D01-D14 / RB01-RB16; the historical tracing and current delta are in
`cloudflare/okx-mcp/FINANCIAL_WITNESS_RETIREMENT.md`.

Previous checkpoints below are historical evidence, not extra current scope
or permission to install/deploy/trade. Current closure rules supersede their
old performance targets and #512 gate dependencies.

---
## #510 r5 - Native lifecycle and financial authority retirement (Draft / NO-GO)

One Rust Engine, one financial SQLite, one bounded writer. New initialization,
verified account connection, campaign policy, execution, financial projection,
RiskBook and recovery use the same original SQL owner. No installed C:\okx data
was read or changed by these isolated contracts. No deployment or trading.

### Production contracts now exercised directly

- Initial empty policy and full desired roster may arrive BEFORE account
  connection. NativePolicyObservation separates observation from strict SQL
  application. Pending policy creates no campaign, reserve or trade permission;
  foreign installation/demo scope rejects. After the original signed account
  connection, strict application/maintenance can create the authorized policy.
- Account publication failure is explicit and recoverable by the SAME connect
  request. SQL remains durable; unpublished source tasks are stopped/joined.
  The failure contract forces a real filesystem obstruction and proves original
  opening reuse, one account, no grant and no POST after exact retry. This does
  not claim arbitrary external Windows file locks can never reject publication.
- Public Native strategy Create/List/Status/Start/Stop/Update, replay/conflict,
  CAS and immutable versions use typed original scoped SQL. Automatic decision
  claim/no-trade retain policy retry lineage and terminal idempotency. No legacy
  AccountStore closure runs through these strategy entrypoints.
- Public Native Execute performs one Create; lost ACK becomes UNKNOWN; repeated
  Execute and Status use original readback, never another Create.
- The filled lifecycle captures real raw fill/bill observations, projects the
  original attributed lot/NAV, verifies attached protection, settles entry,
  sends public Close once, resolves its lost ACK, captures its fill/fee, cancels
  protection and releases risk. Cold reopen retains zero reserve, spent entry
  1.0005, equity 99.999 and exact fees 0.001. Individual run PASS in 14.35s.
- A REAL CHILD PROCESS initializes a new Native installation and performs public
  Execute. Parent kills it after the single Create and before outcome commit.
  Reopen preserves POSTING/fence/reserves; original recover_posting makes UNKNOWN
  without changing money, repeated recovery is idempotent, reconnected public
  Status finds the same order and sends no POST. Individual run PASS in 6.51s.
  This is one crash boundary, NOT all F0-F8 and NOT power-loss durability.

### Cloudflare responsibility

Active governance/configuration/auth/MCP/transport no longer imports the old
risk_book or legacy_finance_cutover modules. Old financial RPC always denies
NATIVE_FINANCE_AUTHORITY_REQUIRED after session/auth fences, without storage
access or a financial grant. Existing DO financial keys are not reset/deleted.
Pure decimal validation is separate from historical financial code.

npm run check: 9 retained Worker test files PASS. Actual workerd physical check
PASS: token/session/generation, obsolete finance denial, ping, dispatch ACK and
response, persisted SQLite generation restart. This is transport evidence only.
FINANCIAL_WITNESS_RETIREMENT.md maps all 26 historical IDs to actual Rust
properties and labels gaps NOT_PROVEN. Old financial successes are not reused.
The required Linux/Worker CI uses new transport and Native component checks in
place of retired Worker money traces/shared-cap authority. Full Windows T09/T10
matrix still needs property-based adaptation; it remains mandatory and is NOT
marked green or skipped by this change. Ready/merge remains NO-GO.

### Current local validation and limits

Complete execution library with fresh owner queue: 331 PASS, 0 FAIL, 2 ignored historical probes, 9.59s.
Fresh Native aggregation: 7/7 PASS, 14.68s including actual child death and full
filled/Close/release/reopen. Named Agent groups plus startup: 98 PASS total
including those seven. Worker check and actual workerd transport PASS.
These are separate targets; there is no combined <=10s claim.
Named Agent regression groups and startup checks PASS before final aggregation;
final exact-tree run results are recorded on #510. Formatting, architecture plus
negative self-tests, SQL schema guard and workflow YAML are required unchanged.
Build time is separate. The filled and crash probes above are independent times,
not a combined <=10s claim. #512 whole-matrix P95<=10s is not yet demonstrated.

Additional current contracts: STOP after fill denies a new Open without budget
mutation or POST but completes source-verified Close/release/reopen (14.32s).
Partial raw fill without its bill cannot bind settlement; late linked bill
settles 0.4002, next fill reaches 1.0005 and replay does not debit again; live
exposure remains reserved (12.33s). This does NOT prove partial-stop resizing.
Fresh owner queue contracts prove capacity128/FIFO drain, pre-enqueue/queued
deadlines and dropped callers using read-only jobs; financial commit cancellation
is proven by the separate original Create/crash tests, not by atomic test markers.

Final new-owner aggregation after STOP/partial additions: 9/9 PASS in 14.99s
on Windows after build. Full execution: 331 PASS, 0 FAIL, 2 ignored in 9.59s.
Both targets remain separately timed; complete 26/26 and P95 are NOT claimed.
The same shared raw fixture serves filled and partial scenarios; no duplicate
financial test implementation or looser stop verifier was added.

### Remaining before #510 GO

1. Final exact-SHA CI on this candidate; do not combine results across SHAs.
2. Remaining fresh-owner negative/physical properties identified by the 26-ID
   map: native source failures/partial and late fills/fee changes/STOP/fairness,
   distinct crash boundaries and transport child death. Old AccountStore/import
   fixtures are component evidence, not replacements for new-owner callers.
3. RB01-RB16 evidence and measured speed; diagnostics busy/restart and immutable
   source responsibilities remain explicit. No claim that ordinary lifecycle
   alone proves every negative financial path.
4. Retire unreachable legacy runtime/storage commands after their surviving
   user-facing operations have one verified Native implementation; no backward
   compatibility, historical import or second financial owner in production.

Do not close #510 or mark PR ready based on a small subset. #512 mapping may be
prepared now; its new complete runner cannot hide unfinished #510 properties.

## #510 - Native Status and stopped legacy finance boundary (Draft / NO-GO)

Baseline b08a24b6; one parent-integrated batch with code subagents. Supersedes
that checkpoint's list of unported Status operations, NOT its NO-GO decision.
Target remains ONE installation financial SQLite/SQL writer. Legacy adapters
remain transition compatibility only, never a second simultaneous authority.

Production Status now routes cleanup, protection receipts/incidents/resolution,
effective protected quantity, partial/terminal/late settlement and ACK readback,
no-send release, terminal Resolve and local Close through existing scoped owner
APIs. Stop resizing after Reduce uses the same original billed-reduction proof
and coherent original lot/opening/receipt snapshot. Emergency/public cancel
markers use the typed existing durable operation owner. No duplicate venue POST.

The original three raw financial validators and four settlement binders were
extracted from large finance/intents methods into domain source modules; legacy
methods delegate to the same implementation. Static scoped CTE reads isolate
all fill/bill/fee joins, exact decimals and original digest/revision checks.
Native SETTLE already writes account ACK with RiskBook; subsequent ACK calls now
VERIFY the accepted original durable receipt read-only, never write another ACK.
No-send release commits its original account ACK with the RiskBook operation.
Native terminal release uses original ResolveTerminal plus account release ACK
in one COMMIT; lost replies require matching immutable readback.

Terminal peers in census are now replayed through the FULL original immutable
fill/bill/fee validator inside the same snapshot, with complete fresh source
frontiers and no own lots. Source replay must perform no writes. The Agent uses
verified peers ONLY after independently proven venue-flat/no-algos/no-orders.
No cached receipt or synthetic complete flag substitutes these facts.

Native terminal release has a DIFFERENT receipt hash domain than historical
legacy ACKs. Protection resolution checks exact risk_entries release_request_hash
and release_proof_digest for native release, scoped to the same original intent,
reservation and plan. Imported archived legacy ACKs retain their original domain;
a present native release must match its authoritative fields.

Local evidence after this batch:
- Full execution library 314 PASS, 0 FAIL, 2 ignored, 9.84s after compilation.
- Agent addressed regression contracts: 78 PASS; startup binary: 5 PASS.
- Agent build, architecture guard+self-test, ledger schema, fmt, diff-check and
  CI YAML parse PASS. Existing warnings remain; no warning-free claim.
- New original SQL contracts cover same IDs across accounts, missing foreign
  bill/cursor, partial/terminal/reopen, immutable ACK, zero-fill rollback,
  peer-source replay without writes, local Close, billed Reduce and protection
  incident/receipt isolation. Bad fixture durability/receipt ownership were
  corrected; no FK, immutable-plan, source or risk guard was relaxed.
- Existing short CI now includes cleanup/terminal/unsent regressions and rejects
  zero matching tests. GitHub run for THIS candidate still needs verification.

RB09 server boundary implemented after Status commit 65c4e353:
- Original RuntimeSession/DO owns an irreversible retirement receipt; authenticated
  owner-only disable request must match the actual deployment source tree.
- Original RiskBook and policy mutation transactions check retirement inside
  their transaction; all nine WSS risk actions deny after retirement. No second
  budget, coordinator, grant service or recurring Rust hot-path RPC was added.
- Readback exports ALL original durable keys, including unknown old formats,
  policy receipts and obligations, except transport generation and retirement
  itself. Pagination is complete or fails; SHA and count are independently checked.
- Exact replay retains original receipt; conflicting/corrupt markers fail closed.
  Snapshot is RAW source evidence, NEVER an EMPTY or Rust activation assertion.
- npm run check: 12 Worker test files, TypeScript, S2 and release bindings PASS.
- node scripts/test-legacy-finance-cutover.mjs: physical workerd + persisted DO
  SQLite cold reopen PASS, 18 native WSS denials across both lifetimes, independent
  snapshot SHA/readback, owner auth and deployment mismatch negatives. Last local
  run 892ms INCLUDING bundle/setup/reopen/teardown; no P95/full-matrix claim.
- Existing draft CI runs this bounded witness and Worker regressions, not another
  full historical Native/Worker campaign. No cloud deployment was performed.
- Rollback to pre-guard Worker code is UNSAFE/UNSUPPORTED. The durable marker
  cannot stop old code that ignores it; permitted deployment rollback must retain
  the guard. Same-code cold reopen is proven, arbitrary rollback is NOT proven.

Exact next production blocker (not a timing/fixture hypothesis):
The present stopped importer takes original campaign SQLite v5, NOT the TS DO
snapshot. TS RiskEntry lacks original native_incarnation, Reserve book_sequence,
BeginPost post_sequence/post_source_digest/post_existing_occupied_usdt and native
request_digest. Later transition_proof may overwrite the earlier proof. Native
v5 integrity requires those values. Do NOT synthesize incarnation, sequence or
hashes. Support explicit imported legacy provenance under the SAME SQL owner,
recover missing facts only from original account durable records, and reject
unsupported history; retain all money/UNKNOWN and original immutable snapshot.
A nonempty legacy source is not an empty new campaign. This mapping/parity is
still NOT_PROVEN; server disable alone MUST NOT activate Rust.
Active reopen is also unfinished: offline inspection compares current rows to
initial import digest. Before live activation, separate immutable import receipt
from active integrity/roster/campaign checks so the first valid commit does not
break restart. Activation must bind verified stopped source + complete import;
never authorize it by a caller-supplied boolean or plain digest.

Still NOT accepted: positive fully activated Native Engine Execute -> Status ->
Close, concurrency/lost-ACK/reopen on that real runtime composition, RB09 isolated
server-disable/import/activation, all-nine-action/final required exact-SHA gates,
full T09/T10 26/26 and direct-target measured P95. The activation gate remains
closed. This code/test batch does NOT manufacture a live activation or financial
source, and does NOT claim installed Windows, physical or live-market acceptance.
RB09 and final production-path proof are the next acceptance boundary; do not
start another helper-only phase or call these unit counts product readiness.
No merge, deployment, installed-data changes or trading performed. PR stays Draft.

## #510 - scoped runtime admission/capture/recovery port (NO-GO remains)

One integrated batch, baseline c5865232, with three code subagents and parent
runtime integration. This supersedes the first unfinished-boundary paragraph
in the historical checkpoint below; it does not claim the whole runtime done.

ExecutionRuntime now has exclusive ExecutionStore::Legacy/Native composition.
Native contains only the existing installation writer plus account scope, never
another AccountStore/Connection. Unported legacy closures explicitly refuse
Native access; there is no Deref, hidden second writer or Cloudflare fallback.
Native storage diagnostics honestly return NOT_EVALUATED when no derivative
projection exists, while queue/owner timings are actual installation-owner jobs.

Production routing now covers scoped prepare/manual definition, quote+optional
ALL-IN evidence, reserve-attempt and pre-reserve rejection, account/strategy
financial admission, campaign readiness/history, coherent local census, source
capture, order readback, outcome recording and posting recovery. Close/Reduce
use the original local receipt/CAS and a consuming post-COMMIT claim; they do
not mint campaign risk. Legacy callers reuse the extracted original rules.
Native capture has real elapsed public-operation timings; unavailable per-job
queue/SQL timings remain None, not invented zero-duration SQL measurements.

CampaignCapture is a private domain borrow: original source/continuity/readiness/
baseline/delta rules, static scoped SELECT CTEs preserving rowids, explicit
scoped write keys, caller-owned transaction, no second COMMIT. Unused forwarding
wrappers were removed. One missing namespace binding was found by independent
review and fixed before testing. Immutable plans and FK constraints remain on;
new test fixtures were corrected rather than disabling their protections.

Evidence on this local candidate:
- Full execution library: 305 PASS, 0 FAIL, 2 ignored; 8.23s after compilation.
- Agent admission, mapping, source, plan, sender/entry settlement, strategy,
  cleanup, terminal/unsent release, identity and risk routing: 70 PASS.
- Agent binary native/stopped startup selection: 5 PASS.
- Agent all-target check, fmt, architecture guard+self-test, original ledger
  schema check, CI YAML parse and diff whitespace: PASS.
- New SQL chain uses original prepare/quote/admission source, joint Reserve,
  BeginPost, close/reopen, UNKNOWN and ACK, rejects repeat authorization. It is
  explicitly OFFLINE: account-only import has NO migration/activation manifest,
  no send capability is manufactured and no positive live grant is claimed.
- Original posting recovery tests retain charged capacity, isolate accounts,
  roll back all recovery on a broken second account and reopen WAL/FULL state.

The existing draft CI now runs the new short Agent boundaries and refuses zero
executed tests. Full required Windows/T09/T10 CI has NOT been run for acceptance;
these direct counts are NOT 26/26, RB01-RB16 completion or P95 <=10s proof.

FIRST REMAINING PRODUCTION BOUNDARY: ACK -> full Status protection/settlement/
terminal release. These remaining account-only operations explicitly refuse
Native rather than using legacy SQL. Terminal-peer census facts remain UNVERIFIED
until the original raw fill/bill/fee settlement validator is scoped and replayed.
The new explicit native startup route checks original import/authority BEFORE
credentials/resources; the unchanged RB09 gate currently refuses activation.
Selecting --native-finance-campaign cannot activate finance. Existing stopped
selection remains read-only and mutually exclusive. No installed runtime changed.

Complete Native Status and terminal source validation, connect Resolve, prove
isolated RB09 and remove old financial authority, then perform positive actual
Native Execute concurrency/lost-ACK/reopen and exact-SHA required acceptance.
Do not use trial_* or fabricated activation rows to claim that acceptance.
PR #511 remains Draft; no merge, installation, deployment or trading performed.

## #510 - Native admission integration candidate (NOT accepted end-to-end)

Supersedes the unfinished-ingress notes in the historical checkpoint below.
The production Execute branch now passes its produced Reserve/BeginPost to the
single installation writer, receives the original joint-commit reservation
receipt, consumes the first-COMMIT send claim, and routes ACK/REJECTED/UNKNOWN
to that same owner. Native does not call a second account reserve or begin_post.
An unknown native admission outcome cannot enter legacy rejection/release.
Produced native admission cannot fall back to the Cloudflare route.

Contract fixes found during independent review:
- SQL rederives USDT/USD loss from the immutable plan and stored admission FX;
  a self-consistent but smaller caller-selected reserve is rejected.
- Shared native sizing keeps ceil9 USDT and the original exact RiskAmount USD
  product. The internal producer carries this exact USD separately from the
  unchanged legacy nine-place transport envelope. Non-parity FX is tested.
- Census is checked against original scoped account policy/loss and fresh
  COMPLETE NAV in the same SQL transaction. Foreign/incomplete/stale sources
  and unsupported nonzero external occupancy are rejected without writes.
- The send capability is created only AFTER the joint COMMIT succeeds.

Local evidence: execution library 281 PASS, 0 FAIL, 2 ignored, 7.19s after build;
Agent producer 3 PASS, census mapping 2 PASS, entry/sender 8 PASS (0.29s),
strategy contracts 3 PASS (0.13s), native generic-mutation guard 1 PASS.
The counts and timings are NOT Native Execute acceptance, full 26/26 or P95.

FIRST UNFINISHED BOUNDARY: actual Native runtime composition, not another helper.
ExecutionRuntime.store remains AccountStoreHandle. open_stopped_finance creates
no trading routes. The RB09 authority gate correctly rejects all live commits.
Therefore this candidate has NO accepted positive end-to-end Native Execute.
Do not activate it, invent a live marker, use trial_* as production, or open a
second account writer to make the positive path reachable.

Complete the existing runtime port as one unit: scoped prepare/manual-instance,
quote+ALL-IN binding, reserve-attempt and no-send outcomes, coherent obligation/
protection/receipt census snapshot, source capture and posting recovery. Retain
original source validation and the same SQL transaction for related facts.
ACK currently continues into Status, so that continuation must use the same
owner too. Pure readonly helpers or an enum alone do not finish this boundary.
Then prove concurrent Execute, lost reply and reopen through the actual runtime
without another grant/POST. Resolve, isolated RB09, removal of old financial
handlers and exact-SHA complete acceptance still remain. PR stays Draft/NO-GO;
no Windows installation, Cloudflare deployment or trading was performed.

## #510 - Native local reservation receipt contract (no wire HELD hash cycle)

A private ReserveNative transaction now derives CentralReceipt from ORIGINAL
scoped intent/decision and reserve proposal instead of asking Agent to hash a
not-yet-delivered RiskResult. Domain hash preimage is the explicit JSON tuple:
["okx.native-reservation-receipt/v1",installation,campaign,account,intent,
reservation_key,plan_hash,policy_revision,decision_owner,canonical_loss_usd,
conversion_digest]. SHA-256 output remains 64 lowercase hex characters.
This digest binds local durable identity; it is NOT external source authentication
or a venue POST grant. Legacy remote RiskResult hash semantics remain unchanged.
No clock, native incarnation, response sequence or transport serialization enters
this identity. Policy/campaign/owner/amount validation and budget/account updates
still run through the original single-transaction reserve implementation.

The existing two-account joint-reserve contract now also exercises ReserveNative:
local receipt stable across envelope time/incarnation, distinct from the legacy
fixture response hash, wrong plan/foreign installation reject, first reserve
and exact retry leave one risk entry with the exact locally derived receipt.
The isolated transaction rolls back before all original OS-kill/reopen tests.
A first negative used an accidentally identical hash; fixed the test to guarantee
a different value, with production checks unchanged.

Execution library: 273 PASS, 0 FAIL, 2 ignored, 10.02s after build. This is a
single full-library timing, NOT the direct-target P95 <=10s performance proof.
Agent all-target check, architecture guard/self-test and format/diff PASS.
No Agent test binary rebuild was needed for this private execution-only command.

Still NOT implemented: public source-bound Native Reserve ingress must validate
current actual census/ALL-IN admission and connect this domain receipt to native
HELD/readback without another account reserve. Current command accepts private
ReserveInput/VerifiedCensus only; it is not exposed on the wire or activated.
Runtime composition, BeginPost producer + consuming sender, Resolve, RB09 and
full exact-SHA acceptance remain. Do not count this backend contract as an
accepted end-to-end Native Execute.

## #510 - combined source/policy/sender integration batch

One parent-integrated batch from two code subagents plus sender integration.
Typed AccountPolicySnapshot/CentralReceiptView now back original legacy methods
and scoped Native reads; actual TradeService policy consumers use named fields
instead of positional tuples. The original active-exposure receipt predicate,
exact decimals, campaign identity and retained-vs-active semantics are preserved.

AccountReserveSource reads intent + original quote + ALL-IN admission under one
Deferred snapshot. The original four quote/admission validators were extracted
into source_entry_admission with scoped SQL; no alternate financial arithmetic.
A known quote still cannot stand in for missing all-in admission. Native Reserve
mutation is NOT implemented by this read API and these data are not a grant.

Submission has one common send/outcome routine. Legacy obtains its own claim;
the staged Native entry consumes the already committed claim without a second
account CAS, checks the real owner installation before any callback, and records
UNKNOWN or proven-unsent via the selected owner. Native first-COMMIT operation
now samples clock inside the owner and preserves typed original denial reason.
The Native entry remains unselected by Execute until Reserve/fence integration.

New direct tests: two-account policy/receipts with same local IDs and different
campaigns/plans; retained RELEASED evidence vs active None; no read-side changes;
scoped quote/admission equality and forged fee/upper rejection. Sender contract
adds real SQL lost-ACK -> UNKNOWN and replay no-send; pre-send policy denial ->
REJECTED; dropped committed claim cannot be reissued. Sources/venue callback are
synthetic, original SQL state transitions are real. Two new fixture setup bugs
(campaign mutation, missing WAL/FULL) rejected first run; corrected at setup,
with production invariants unchanged.

Execution library: 273 PASS, 0 FAIL, 2 ignored, 8.89s after build. Agent all-target
check and architecture negative/normal guard, format/diff PASS. Sender targeted
contracts: 8 PASS, 0 FAIL, 0.24s after one 3m42s Agent test build. Production
Strategies/governance contracts: 3 PASS, 0 FAIL, 0.23s on the SAME binary. No manual full GitHub CI was launched.

Critical next contract: existing CentralReceipt.receipt_hash is digest(RiskResult)
constructed AFTER remote HELD; unified reserve needs the local receipt DURING
its same COMMIT. Define and independently test the Native durable reservation
receipt contract before wiring the action; do not invent a fake remote response
or a second reserve. Update Native local receipt readback and sender together.
Also remaining: actual runtime composition, source-bound BeginPost/Resolve,
activated producer-to-owner positive tests, RB09 and final exact-SHA acceptance.
This batch does NOT complete #510 or the 26x matrix.

## #510 - consuming send capability at the real legacy sender

CommittedVenueSend is an ephemeral owned result: private fields/constructor,
no Clone or serde, bound to installation/account/intent/plan. Its send(self,
FnOnce) consumes the value. Legacy claim_legacy_send reads the original plan,
rejects a mismatched expected plan BEFORE CAS, performs original begin_post,
and constructs the value only after that method's COMMIT. post_once consumes
it; deadline and owner-confirmed-unsent/UNKNOWN outcome handling remain.
There is no constructor from POSTING, Inspect, stored receipt or reopened state.

A PRIVATE native commit_native_post path is staged: same original gate and
combined risk/account transaction, scoped lookup, COMMIT before minting the
value, lost response drops it. It is NOT called by Agent yet and does not
establish an accepted Native positive send. Original public risk actions stay
fail-closed; native Reserve producer and first-COMMIT/sender integration remain
required. These staged methods must not be accepted based on legacy tests.

Execution library: 271 PASS, 0 FAIL, 2 ignored, 7.50s after build. Agent all-target
check and architecture guard/negative self-test pass. Direct sender scenarios
exercise wrong-plan zero-send, competing callers, repeat no-send and original
partial/terminal/late settlement behavior: 7 PASS, 0 FAIL, 0.21s after a
2m24s Agent test build. Native positive COMMIT/send is NOT_PROVEN.
No full current-SHA 26x, RB09 or P95 acceptance is claimed.

## #510 - Native opening packet retention and actual stopped-owner gate witness

NativeFinanceRisk::install_produced_opening now consumes ProducedOpening itself,
not separately supplied wire command/census arguments. The internal producer
packet remains privately constructed by TradeService after its actual cohort
read. NativeRiskClient passes it intact to Local/LocalStopped and only extracts
the command for the unchanged legacy transport route.

The original real stopped-import owner/lock/reopen contract now calls the public
install_reported_opening API with manually constructed DATA DTOs. It must return
FINANCE_CUTOVER_NOT_ACCEPTED before any opening/source mutation, leave the exact
campaign summary unchanged and retain OFFLINE_NOT_AUTHORIZED. The same original
contract continues to verify import integrity/reopen/exclusive OS owner behavior.
This proves the stopped gate only; input DTOs do not prove live observation
origin, and a future activated API still needs authenticated producer acceptance.

Target owner test: 1 PASS, 0 FAIL, 0.71s after build. Full execution regression:
271 PASS, 0 FAIL, 2 ignored, 7.42s after build. Agent all-target check and
architecture/negative guard, format/diff PASS. No Agent test binary rebuild or
full GitHub CI was requested for this signature-only packet change.

Still NOT_PROVEN: actual producer -> activated Native opening -> durable COMMIT
-> exact Summary as one accepted integration test. Do not count prior bridge
unit tests as that witness. Remaining Native actions/runtime/one-use POST/RB09
and current-SHA final acceptance continue to block #510 completion.

## #510 - opening producer-to-Native handoff (activation remains closed)

TradeService no longer discards its real whole-cohort census before opening
installation. Private ProducedOpening is constructed only by the TradeService
producer after census/command validation. NativeRiskClient dispatches this
packet locally or preserves the existing legacy wire command. Native ingress
validates installation/campaign/account roster and converts only observed
amounts/times/digest, never a caller-provided financial generation.

Execution receives public DATA DTOs NativeOpeningSource/Account and
ReportedOpeningCensus; these are not source authentication or financial grants.
The existing activation gate remains mandatory. The private SQL binder reads
original risk generation/roster, validates reported account maps/exact money,
source time/digest and calls the original source-bound opening operation in
one transaction. Dispatch obtains clock time inside the SQL owner. VerifiedCensus
remains private. Native returns success only after matching the committed source
digest to the actual Summary opening readback; network failure is not a retry
permission. Generic request(InstallEntryOpening) still rejects without this
internal producer route.

The existing two-account source contract now also exercises the reported binder:
future/stale census, missing account, foreign installation and negative amount
reject; valid observations reach the original operation and rollback cleanly.
Execution library: 271 PASS, 0 FAIL, 2 ignored, 7.74s after build. Agent all-target
check and architecture/negative guard pass. Existing risk_bridge tests: 5 PASS,
0 FAIL, 0.01s after a 2m11s Agent test build. They check legacy risk routing
regressions, not the new positive activated Native opening. This is NOT an activated full Native
runtime test: production AccountResources still selects the legacy store, and
end-to-end positive activation, readback failure, RB09 and full 26x are NOT_PROVEN.

Next critical integration: source-bound Reserve/BeginPost + one-use send handoff,
Resolve Unknown/Open/terminal, remaining account APIs/composition and verified
activation/old Cloudflare finance disable. Do not grant a send from POSTING,
Inspect or restart readback. No installed application state was changed.

## #510 - typed SQL boundary of the production venue sender

post_once now calls claim_legacy_post and record_intent_outcome instead of
passing SQL closures into the Agent send routine. Claim keeps the original
AccountStore CAS and evaluates wall time INSIDE its SQL owner after queue
wait. Typed IntentOutcome carries the observed result; the unified counterpart
uses the existing original RecordOutcome transaction under the activation gate.
No new finance model, send permission from readback or Native activation exists.

The seven original entry-settlement direct tests now prepare their real SQL
intent/reservation using two concurrent post_once callers: exactly one succeeds
and invokes the controlled send callback; a third replay returns None without
another callback. Original partial/terminal/late settlement negatives remain.
This is a real production sender + SQLite contract with a controlled callback,
not physical OKX HTTP acceptance. 7 PASS, 0 FAIL, 0.19s after build. Initial
filename-based filter executed ZERO tests and is explicitly NOT evidence; the
verified module filter is entry_settlement::direct_tests. Final execution
library: 271 PASS, 0 FAIL, 2 ignored, 7.39s after build. Architecture guard and
negative self-test, formatting and diff checks pass.

This prepares but DOES NOT solve the Native single-send handoff. Current
post_once accepts the legacy AccountStoreHandle only. A future Native path
must consume a private non-cloneable first-COMMIT send result; it must not
repeat the account begin_post after the combined SQL fence or accept POSTING,
Inspect, replay or reopened state as a new send grant. Four Native action
bindings, runtime composition and RB09 still block #510 completion.

## #510 - immutable campaign opening provenance and Native Summary readback

Opening installation now commits its canonical full source as a separate
immutable operation receipt alongside the original installed-money marker.
This is provenance in the SAME database/transaction, not another financial
ledger. The original canonical digest calculation is shared with readback.
No schema migration or activated financial capability is added.

Scoped readback verifies the persisted source schema/fields/digest, campaign,
first account, timestamp and original installed-money marker. A missing full
proof (including older imports) remains None: inception must not be inferred
from process time, account T0 or the monetary marker alone. Native Summary
projects entry budget and campaign origin only from that persisted source;
current money still comes from the original RiskBook summary transaction.
The returned opening timestamp is NOT presented as fresh venue evidence.

Existing real two-account opening contract now checks absent proof after
rollback, a forced final provenance-write failure rolling back BOTH installed
marker and budget sequence even if caller commits, full source after actual
COMMIT/reopen and foreign-installation rejection. Original replay/conflict and
source-readiness negatives remain. Full execution library: 271 PASS, 0 FAIL,
2 ignored, 6.96s after build on the final visibility-corrected source. Agent
all-target compilation and architecture/negative guard pass. Not a new full
26x/RB/P95 acceptance result.

Remaining Native ingress: actual produced census/opening handoff; source-bound
Reserve/BeginPost with sender's single-use send ownership (do not repeat the
account transition after native COMMIT); Resolve terminal and Unknown/Open;
remaining AccountResources composition, verified RB09 activation/old finance
endpoint disable and final current-SHA acceptance. Writes remain fail-closed.

## #510 - typed trading reads and governance capital producer

Actual Execute, Status/Cancel/Close, recovery and terminal-release callers now
use TradeIntentLookup and typed active_lots reads (11 standalone SQL closures
removed). Existing find_intent_sql/active_lots_sql decode the original plan and
scope all identities/lots by installation/account. Unified reads use bounded
deadlines and a Deferred transaction; no mutable SQL capability escapes.

Actual Strategies CapitalAssign now supplies CapitalAssignmentCommand with the
already-authenticated governance receipt. AccountStore owns source NAV selection,
exact replay, two-leg commit and result construction in ONE write transaction.
Unified AssignBoundCapital uses the same producer. A stale snapshot still
returns the original receipt with capital=None/PARTIAL on exact replay; a fresh
new assignment must validate identity/policy/source/principal. Activation is
unchanged and closed. Production AccountResources remains on the legacy handle.

Direct tests: same intent/fill IDs with different A/B plans and lot quantities;
all lookup variants, full plan parity, unknown account, SQL column allowlist,
zero read-side changes and reopen. Capital producer: first assignment, exact
replay after NAV expires, hash conflict and foreign account. Existing financial
rollback/reopen witnesses remain in the same regression.

Combined execution library: 271 PASS, 0 FAIL, 2 ignored, 7.99s after build.
Agent all-target check and architecture/negative guard PASS. Production strategy
contracts: 3 PASS, 0 FAIL, 0.15s after build. First run failed before production
entry because simultaneous Windows fixtures reused clock-derived directory names;
random 128-bit names fix that collision without fixture-data or assertion changes. No full 26x,
RB01-RB16, cutover or P95 acceptance is claimed.

Focused subagent review found a concrete next wiring dependency: opening's
production caller currently discards its computed census, while native opening
requires it. Preserve that actual producer evidence instead of inventing a
VerifiedCensus from JSON flags or SQL-only data. Native reserve already commits
AccountStore reserve together with HELD, and native BeginPost already commits
POSTING. Existing TradeService repeats those transitions after remote replies.
The next Native ingress work MUST update sender/claim ownership together:
HELD -> one durable POSTING -> exactly one actual POST; repeat/UNKNOWN -> no
second send. Resolve needs terminal source binding AND Unknown/Open behavior;
the terminal-only SQL helper does not implement those other transitions.

## #510 - typed production Portfolio snapshot and scoped financial reports

Production portfolio.rs now calls FinanceReportQuery -> FinanceReportSnapshot,
not an Agent-provided AccountStore SQL closure or positional six-tuple. Health,
filtered history/attribution, NAV, T0 and ledger revision share one Deferred
read transaction. The existing AccountStoreHandle and UnifiedFinanceHandle
expose typed report methods; SQL does not escape okx-execution. The current
Agent composition still selects the legacy handle, not a new active owner.

Original algorithms are shared by explicit source_nav_report (224 lines),
source_finance_report (65) and source_financial_risk (57). All unified table,
receipt and revision reads are scoped by installation/account. Strategy NAV
requires the original immutable receipt and matching event-time revision;
manual remains NOT_APPLICABLE. Filtering, checked decimals, pagination,
freshness and SQL interruption stage codes are retained. No risk grant or
new financial data source is introduced by a read report.

Direct witness: legacy A report equals unified A; two account principals
produce 0.1/0.2 strategy PnL. A matching foreign A receipt cannot prove a forged
B NAV point. One-row filtered history resumes without omission; revision is
from the same account, and a newer B account mark invalidates B strategy NAV
without invalidating A. Unknown account/backward range reject. COMMIT/reopen
retains the original A result. Source data are isolated synthetic fixtures.

Windows execution library: 270 PASS, 0 FAIL, 2 ignored, 8.17s after build.
Direct report witness: 0.33s. Agent Portfolio tests: 13 PASS, 0 FAIL, 0.30s
after build. Agent all-target check and architecture/negative guard plus
format/diff PASS. Each test binary was built once for this slice; the full
execution regression reused the direct test binary. No full GitHub campaign
was manually launched. These results are not 26x/RB01-RB16/P95 acceptance.

Remaining integration order: complete original trading/governance/source
producer typed APIs and bind AccountResources to the one owner; finish the
four source-bound Native risk actions; implement verified one-time activation
and disable/remove old Cloudflare financial authority; then full same-SHA
functional/physical/regression and performance acceptance. Do not select a
partially ported owner or turn the activation gate into an unconditional allow.

## #510 - shared strategy valuation and conserved capital operations

Original AccountStore now delegates to explicit SQL modules: source_capital
(principal/current or historical as-of), source_valuation_finance (bounded
current totals and historical filtered fold), source_strategy_valuation
(source/position/FX reconciliation), source_strategy_nav (immutable revisions,
NAV and receipts), source_capital_assignment (two conserved governance legs).
Source identity/receipt decoders are shared, not replaced with trust flags.
All unified queries and joins bind installation/account scope explicitly.

CommitStrategyNav runs reconciliation, receipt/NAV publication and the coverage
cursor under the installation command transaction. Its cursor must name the
strategy valuation stream, exact as-of and account snapshot generation.
AssignCapital keeps account identity, source NAV, policy revision, available
principal, chronological assignment and idempotency checks in the SAME write
transaction as both opposite economic legs, totals, provenance and receipt.
Neither command reserves venue risk or creates strategy risk highwater.
Public Native APIs remain behind the unchanged activation gate.

Direct strategy witness: same IDs on two accounts with different entry prices
and principal. Exact original results match, including 0.1/0.2 unrealized PnL.
A later capital assignment is excluded from historical as-of principal; filtered
SQL parameters, stale NAV, recovery isolation, foreign evidence, missing mark/FX,
wrong cursor, pending foreign projection and event-time revision are checked.
Cursor failure after tentative NAV/receipt inserts rolls back both. Replays and
COMMIT/reopen preserve exactly two NAV points/receipts per account, no highwater.

Direct capital witness: A receives +3 to principal 28; B releases -7 to 38,
with the same operation ID. Foreign identity, stale policy, backdating,
over-assignment and over-release reject. A test-only temporary SQL trigger
rejects the final receipt AFTER both legs/totals: original finance frontier,
principal and receipt absence are unchanged. Replay writes no extra legs;
changed request hash conflicts; account net capital stays zero, provenance
sealed and historical principal 20/40 survives COMMIT/reopen.

Windows execution library: 269 PASS, 0 FAIL, 2 ignored, 8.02s after build.
Each new direct witness passed in 0.22s before additional strategy assertions;
those pass in the full regression. Full 26x, RB01-RB16, P95 and cutover acceptance
are NOT established by these unit/SQL results. Source facts/auth receipts in
fixtures are synthetic and do not prove live producer authorization.

Remaining: user-facing NAV/report queries and other original AccountStore
producers, new-owner AccountResources composition, four Native risk actions,
authenticated activation/old Cloudflare financial path removal and final
same-SHA acceptance. Native strategy/capital commands are implemented, but the
production Agent composition still selects the legacy AccountStoreHandle.

## #510 - shared account NAV arithmetic and atomic scoped projection

The collector now calls build_account_nav_page in okx-execution instead of
owning a second account/currency NAV calculation. AccountNavSnapshot carries
typed Decimal values; the pure function derives PnL after external flows,
per-currency NAV and the matching account_snapshot cursor. Empty source
metadata, duplicate/empty currencies and arithmetic overflow reject the input.
This read model is not an execution grant or independently verified venue proof.

Original opening equity, USD component and cash-flow reads share SQL helpers.
Legacy cash-flow caching and statement timings are preserved. Unified reads
scope every table by installation/account. ProjectAccountNav reads those facts,
builds NAV and commits points/highwater/cursor in one command. COMPLETE requires
all six same-generation historical cursors through as_of without gaps; PARTIAL
retains FINANCIAL_BACKFILL_PENDING. Activation remains closed.

Direct tests: pure arithmetic excludes a 10 USDT deposit from 117 equity over
100 opening (PnL 7); duplicate currency and overflow reject. Two imported
accounts reuse the deposit ID but have amounts 10/20 and equity 117/141;
COMMIT/reopen yields independent PnL 7/21 in account and currency views. Missing,
foreign and wrong-generation coverage cannot produce COMPLETE; replay is
idempotent and conflicting immutable NAV cannot replace facts.

Windows execution library: 267 PASS, 0 FAIL, 2 ignored, 6.37s after build;
Agent all-target check, architecture/negative guard and format/diff PASS.
Selected source_capture Agent tests: 8 PASS, 0 FAIL, 0.00s after build.
Two direct NAV tests: 0.19s after build. This is NOT full 26x or P95 acceptance.

Remaining: strategy NAV/capital/reconciliation, new-owner AccountResources
startup, activation and four native risk actions; remove the old Cloudflare
financial path and perform final same-SHA acceptance. Current production
composition still selects AccountStoreHandle; only the shared account NAV
builder is already called by that collector, not the new unified command.

## #510 - atomic pending-fill FIFO projection and raw-close PnL repair

Original AccountStore and the unified writer share source_fill_projection:
bounded pending reads (max 1000), original plan/lot decoders, fee identity and
the existing reduce_fifo calculation. All unified reads and joins are scoped
by installation/account. ProjectPendingFills reads sources, updates lots,
records finance and acknowledges pending fills in ONE command transaction;
an error rolls back earlier fills in that batch. Public handle remains behind
the unchanged activation gate; no trading capability is granted by this work.

The direct test exposed a production defect: after raw capture, a closing
fill already existed, so the old if !existing skipped REALIZED_PNL while
reducing its lots. RED was QueryReturnedNoRows for the expected PnL. Projection
now records that component whenever the pending reduction is applied; replay
without pending returns early and immutable economic keys prevent duplication.
This intentional financial behavior correction affects the legacy caller too.

Witness: two accounts reuse the same intent/fill/lot IDs with different contract
values. FIFO leaves one unit of the second lot; independent arithmetic gives
30 and 60 USDT PnL. A later over-close rolls back all earlier lots, PnL and
pending ACKs; unknown account and zero limit cannot project anything. Bounded
continuation, replay, COMMIT/reopen and the legacy raw-fill production entry
are checked. Test file names include a process-run nonce to avoid Windows PID
reuse opening stale test databases; no previous files are removed.

Local Windows execution library: 265 PASS, 0 FAIL, 2 ignored, 6.35s after build.
The direct witness passed in 0.24s before additional differing-account
mechanics assertions, which pass in the full regression. These are NOT full
26x, RB01-RB16, physical cutover or P95 acceptance results.

Remaining: account/strategy NAV reconciliation, new-owner resource startup,
activation and four native risk actions; old Cloudflare financial path removal
and final acceptance. Production Agent still selects legacy AccountStoreHandle;
the new unified projection command is implemented but not selected by it yet.

## #510 - scoped historical funding projection and atomic attribution command

Original AccountStore funding attribution now reuses source_funding_projection
for pending reads and event-time exposure. The SAME allocate_funding math is
used; unified joins scope fills/intents/decisions/plans by installation/account
and retain f.time <= funding event time. Shared latch_stop_sql retains original
stop storage, no second ledger or authority.

Unified AttributePendingFunding is bounded at 100 pending events and requires
all six historical cursors, empty gaps, coverage through each event and equal
generations. It prepares all USD-normalized pages before writing any allocation;
a missing rate records only the USD stop, leaving every event pending. All
allocations, money and ACKs then commit together. Unknown attribution preserves
account debit and adds FUNDING_ATTRIBUTION_INCOMPLETE, not false ownership.
Legacy caller's existing completeness check and per-event commit behavior are
preserved; the new unified command has an explicitly atomic batch boundary.

Direct test uses original owned account preparation/reserve/POST/ACK and raw
bill/fill capture. A known manual exposure receives -0.2; B's unowned exposure
receives -0.2 unattributed with a stop. A later unowned fill at t=20 cannot
change funding at t=10. Missing gap/mixed generations reject; a second BTC
funding lacking FX cannot commit the earlier USDT event. Repeat is count 0;
COMMIT/reopen retains independent 0.2 loss per account. Synthetic venue/FX
facts do not prove production source authentication or cutover.

Local Windows full execution library: 264 PASS, 0 FAIL, 2 ignored (8.16s
after build); Agent all-target check, architecture/negative guard, format/diff
PASS. New direct funding witness passed before additional batch assertions.
No full RB01-RB16/26x acceptance or installed-state change.

Remaining: pending-fill/position projection, account/strategy NAV reconciliation,
new-owner resource startup, activation and four risk actions. Production Agent
still uses the legacy AccountStoreHandle composition; native funding command
is implemented but not yet selected by that runtime.

---

## #510 - production bill collector submits typed source batches

Agent capture_bills now decodes raw venue fields to CapturedBillPage and
uses AccountStoreHandle::ingest_captured_bills. SQL owner resolves original
order/strategy ownership, entry fee proof, bill/order identity, funding,
transfers, residual classification, USD conversion and atomic page write.
Unified IngestCapturedBills runs the same binder and USD/page dispatcher
inside one transaction. No attribution from a separate asynchronous lookup.

Optional Decimal fields preserve the original absent-vs-zero distinction.
Original funding-PnL and interest ambiguity rules are shared execution
functions; agent regression helpers delegate instead of duplicating them.
Source cursor must be bills/account and batches are bounded at 100 rows.

New direct witness uses original prepare/reserve/POST/outcome accounts,
legacy batch ingestion+replay on A, full import and unified batch on B.
Identical source IDs remain scoped; one 0.01 fee, one 10 USD external flow,
one pending funding and immutable entry-fee receipt per account. Missing
funding amount, foreign order, invalid transfer linkage, ambiguous interest,
oversized page and stale cursor fail. Valid 0.03 interest advances loss to
0.04 once; failure rolls back new interest; real COMMIT/reopen preserves
A/B independent results. Venue/FX acknowledgements are synthetic fixtures.

Local Windows full execution regression: 263 PASS, 0 FAIL, 2 ignored
(6.61s after build); direct captured-bill witness PASS (0.21s). Agent
source_capture tests 8 PASS (0.00s execution after build); all-target check,
architecture/negative guard and formatting/diff PASS. Native resource/collector startup,
fill/funding/strategy NAV projection, live activation and four risk actions
remain incomplete. No RB01-RB16 or 26/26 acceptance claimed.

---

## #510 - production fill collector submits typed source batches to SQL owner

Agent capture_fills now only decodes venue strings/time/fee presence and
submits CapturedFillPage to AccountStoreHandle::ingest_captured_fills. Original
ownership lookup, canonical fill/fee IDs and SWAP labels moved to the shared
execution binder. The unified IngestCapturedFills command runs this binder,
USD normalization and complete page ingestion in the SAME SQL transaction,
removing the need for a separate owner read/attribution gap in this path.
The ordinary legacy owner calls the same binder and original USD writer.

Binder rejects supplied intent ownership, malformed canonical fill ID,
partition/cursor mismatch and >100 rows; skips original pre-T0 facts; uses
current scoped durable client+venue identity. Raw SWAP fee without bill ID
stays raw and cannot become an early canonical debit. No activation bypass.

New direct witness uses original prepare/reserve/POST/outcome account sources,
legacy ingestion + replay on A, real import and unified batch ingestion on B.
It proves scoped ownership, one 0.01 loss per account, forged owner/identity/
cursor/oversize/conflicting order rejection, delayed SWAP fee behavior and
missing-FX stop with no failed fill. Actual COMMIT/reopen retains results.
Venue/FX inputs are explicitly synthetic; not a trading or cutover acceptance.

Local Windows full execution library: 262 PASS, 0 FAIL, 2 ignored (7.14s
after build); direct captured-fill witness PASS (0.24s before added limit
assertion). Agent all-target check and architecture/negative guard,
format/diff PASS. No full RB01-RB16/T09/T10 acceptance.

Remaining collector work: typed bill construction, projection/funding/strategy
NAV reconciliation and AccountResources/ExecutionRuntime new-owner startup.
The current Agent composition still holds the legacy handle; full native
activation and four risk actions remain incomplete.

---

## #510 - source order ownership contract used by the production collector

Fills and bills now use AccountStore::source_order_owner instead of separate
ad-hoc lookups. A shared scoped SQL function resolves durable client/venue
identity and original stored plan. Both identifiers may resolve only the same
intent; a known venue binding cannot contradict a supplied client identity.
Conflict yields SOURCE_ORDER_IDENTITY_CONFLICT before constructing attributed
finance facts. An unbound/unknown order remains None, not an owned position.

The unified FIFO exposes the same read-only operation with account scope and
queue deadline. This readback is not a mutation/POST capability; later source
writes still require source validation and the independent activation gate.
No generic SQL callback or second writer is exported.

Tests on original prepared/reserved/posted account sources with identical
client IDs verify exact client+venue, client-only and venue-only lookup,
foreign venue absence, contradictory client/venue rejection and unknown
account failure. The real unified owner/import test checks public readback
with original immutable plan and foreign-account denial.

Local Windows full execution regression: 261 PASS, 0 FAIL, 2 ignored (8.03s
after build); Agent all-target check, architecture/negative guard, format/diff
PASS. This intentionally tightens old bills' client-first selection and fills'
client-only attribution: contradictory raw exchange identities now fail closed.
Affected T09/T10 production integration still needs final acceptance; recorded
source conflicts must be diagnosed, never hidden by ignoring the new reason.

Native AccountResources/collector full startup, projection/reconciliation,
activation and four financial command arms remain incomplete. No RB01-RB16,
26/26 or installed trading readiness is claimed.

---

## #510 - typed coherent collector frontier on legacy and unified SQL owners

The production collector now calls AccountStoreHandle::source_capture_state
instead of supplying its own SQL closure for opening time and six cursors.
AccountSourceState/Cursors replace the agent-local duplicate shape. The
same AccountStore SQL read implementation serves the unified FIFO through
an account-scoped read transaction. It returns opening time, generation base
and the six original fill/bill cursors in a coherent snapshot. Raw SQL stays
inside execution; no financial capability, cache or second owner is added.

Original-import/owner witness compares legacy and unified account state,
separate A/B generation/cursor values, unknown account denial and expired
queue deadline. No cursor or financial rows are advanced by this read.
Final local Windows execution regression: 261 PASS, 0 FAIL, 2 ignored
(8.98s after build), including all six cursor partitions; direct owner/import
witness PASS (0.51s). Agent source_capture tests 8 PASS (0.00s after build),
Agent all-target check and architecture/negative guard, formatting/diff PASS.
Agent test-binary compilation took minutes and is separate from execution;
these timings are not P95 or full Windows acceptance.

This changes a real production collector call, but ExecutionRuntime still
holds the legacy AccountStoreHandle: new-mode collector/account resource
startup is NOT yet wired. Owner lookup, source projection/reconciliation,
activation/four risk actions and complete RB01-RB16/T09/T10 remain outstanding.

---

## #510 - original USD normalization and durable source-stop outcome

Existing AccountStore::ingest_page_with_usd now reuses normalize_source_page_usd_sql.
Unified IngestSourcePageUsd runs the SAME source existence checks, checked
USD conversion and original conservative loss rounding inside the sole SQL
transaction, then the complete page dispatcher. Existing derived events
are not converted or charged a second time, scoped by account.

For valid conversion metadata but missing/nonpositive rate, the command
returns a typed SourcePageRejected and commits only the same original
usd-conversion-unavailable stop. The public owner maps the committed receipt
to the exact failure reason. No page rows/cursor are applied. Invalid metadata
returns error without manufacturing an observation/stop; other failures use
normal rollback. Live authority gate remains mandatory for this new command.

New isolated original-schema two-account test: 8 USDT * 1.25 -> 10 USD flow;
0.01 USDT fee -> 0.0125 USD cumulative account loss; identical source IDs
on A/B and replay yield one derived event per account. Missing BTC/zero USDT
rate persists only the scoped stop, malformed metadata creates none, and
actual COMMIT/reopen preserves stop reason and last successful cursor.
Synthetic conversion/source inputs do not prove venue identity or activation.

Local Windows: complete execution library 260 PASS, 0 FAIL, 2 ignored
(6.67s after build); Agent all-target check and architecture/negative guard,
format/diff PASS. Direct USD witness PASS (0.17s before extra assertions).

Collector NOT YET SWITCHED: it still needs scoped original owner lookups by
client/venue order, consistent six-cursor/opening read, fill/funding/NAV
reconciliation and native AccountResources startup. USD page ingress is a
necessary part, not evidence these production callers are connected.
RB09/four financial actions/full RB01-RB16/T09/T10 remain incomplete.

---

## #510 - entry fee proof completes the IngestPage field dispatch

Original and unified ingestion share source_entry_fees transaction logic:
posted intent/order/time, same-account bill+canonical fee+source link,
immutable fee proof/index and late settlement requeue. Scoped joins bind all
participating rows to installation/account. No financial authority enabled.
All current IngestPage fields now have an explicit path in source_page;
COMPONENT_NOT_PORTED rejection is removed. Exhaustive Rust destructuring
without a wildcard requires any future field to be handled at compile time.

New isolated test creates two original AccountStore sources through actual
prepare/reserve/begin_post/outcome/release methods (central/venue evidence
is explicitly synthetic), imports both, and calls the same page dispatcher.
It proves foreign-order rejection and rollback, same bill/intent IDs on A/B,
exact replay with one -0.01 fee, immutable proof/index, late-pending state,
early bill/incorrect amount/missing source denial, and cursor error rolling
back a new bill/fee proof. Real COMMIT/reopen retains target receipts/totals;
original source stays unchanged. This is not physical venue/cutover acceptance.

Local Windows full execution regression: 259 PASS, 0 FAIL, 2 ignored (7.15s
after build). After final exhaustive field binding, 20 unified contracts PASS
(1.69s); Agent all-target check, architecture/negative guard, format/diff PASS.
The formerly unsupported-page test now checks the real missing-order reason.

NEXT: source collector/startup wiring, USD/source normalizers and other
account reads/writes still need the shared owner; live activation and four
RiskCommand arms remain incomplete. Whole RB01-RB16, P95 and T09/T10 26/26
are not accepted. No installed-state changes or trading.

---

## #510 - position/NAV projections and fill ACK in unified pages

Original and unified page ingestion share source_positions and source_nav
transaction operations. Lot validation/ownership and immutable NAV source
checks retain original semantics. Unified owned-lot lookup joins decisions
and intents on installation/account/decision; NAV points, peak watermarks,
scopes and pending-fill ACK are scoped by installation/account.

Direct full-schema page witnesses verify identical lot IDs on A/B, exact
replay, adjusted USD peaks (120 equity - 10 flows = 110), A-only increase
(150 - 10 = 140, B stays 110), changed original NAV causing rollback of a
preceding flow, lot side/owner mismatch, negative quantity, attributed
external lot and absent NAV mark rejection. A fill projection ACK cannot
consume B pending fill. The initial peak from imported state is preserved;
test values deliberately exercise a new high rather than assume no history.

The old unsupported-page negative now targets the still-unported entry fee
ownership link; projected-fill ACK is tested positively. Normal new-owner
writes remain activation-blocked. EntryOrderFeeLink is the remaining
unsupported IngestPage field; producer normalization/reconciliation, native
capture/startup and all financial actions are still not complete.

Local native Windows complete execution library: 258 PASS, 0 FAIL, 2 ignored
(7.91s after build); direct page/import witness PASS (0.68s). Architecture/
negative self-tests, formatting and diff PASS. No complete RB01-RB16 or
T09/T10 26/26 acceptance, deployment or trading activation claimed.

---

## #510 - flows, allocations and funding in atomic unified history pages

Original AccountStore and unified page ingestion now share source_flows and
source_funding transaction operations. Immutable cash flows and allocations
retain their original conflict checks. Funding pending and attribution ACK
rows are account-scoped; exact replay cannot re-enqueue attributed funding.
Original AccountStore cash-flow cache invalidation remains after commit.
Unified funding without an original same-account bill fails explicitly with
FUNDING_SOURCE_BILL_MISSING before a generic SQLite FK exception.

Page contracts use two original imported accounts with identical event IDs:
flows/allocations/pending funding replay without duplicates, changed amount
or allocation fails, prior flow/bill inserts roll back on later error, stale
cursor rolls back flow, and A funding ACK cannot consume B pending funding.
The test fixture includes the mandatory raw funding bill; an orphan case
separately verifies its absence and whole-command rollback.

Local native Windows full execution regression: 258 PASS, 0 FAIL, 2 ignored
(7.32s after build); direct original-import/page witness PASS (0.99s).
Architecture/negative guard, formatting and diff PASS. Complete USD producer
normalization, entry fee ownership, positions/NAV and projection ACK paths,
native capture, activation and all actions remain unfinished. No full
RB01-RB16 or 26/26 acceptance is claimed by this slice.

---

## #510 - SWAP source labels and bounded late fee links in unified pages

Original AccountStore page ingestion and unified IngestSourcePage now share
source_swap SQL operations: immutable SWAP labels, direction validation and
bounded late bill/order/trade-to-fill fee linking. Unified joins scope EVERY
participating fills/labels/bills/finance/links row by installation/account.
No new fee debit, copied ledger or source cursor path is introduced.

Direct page witnesses verify two accounts with identical source IDs, exact
replay, same-page bill/fill linking, fill-first late bill, no foreign-account
link, mismatched fee staying unlinked, label conflict and invalid direction
rolling back a preceding new fill. Unported entry fee ownership, funding,
flows/allocations/lots/NAV still reject before writes. Native capture and
activation remain incomplete.

Initial full regression found an existing OS-kill harness readiness race:
parent saw the ready file before its phase bytes and failed F2, leaving a
parked helper holding the Windows test EXE. Replaced that TEST readiness
with a complete stdout pipe message and bounded receive. A local owned-child
Drop guard kills/reaps/joins even on an assertion failure. Actual child OS
kill and original SQLite reopen assertions remain. The verified stale test
helper was stopped; installed OKX processes/state were not touched.

Final local Windows full execution library: 258 PASS, 0 FAIL, 2 ignored
(8.29s after build); direct unified page/import witness PASS (0.69s);
Agent all-target check, architecture/negative self-tests, format/diff PASS.
No complete RB01-RB16/T09/T10 acceptance is claimed.

---

## #510 - unified financial-history page command

UnifiedFinanceWrite::IngestSourcePage and UnifiedFinanceHandle::ingest_source_page
now dispatch supported original fills/bills/bill-order links/finance entries,
account loss, source cursor and finance provenance under the existing single
transaction/savepoint. Source authenticity remains the producer responsibility;
the normal authority activation gate stays enforced. No second SQL owner.

UNFINISHED COMPONENTS: swap fill labels, entry-order fee links, funding,
flows/allocations, lots, NAV and projected-fill acknowledgements still require
scoped paths. A page containing ANY such component is rejected BEFORE writes
with UNIFIED_SOURCE_PAGE_COMPONENT_NOT_PORTED. Nothing is silently ignored.
This is a bounded initial history-page ingress, not full native capture wiring.

Direct full-original-schema page contracts verify A/B same IDs, exact replay,
independent -1 totals, cursor completion, and cursor-regression error rolling
back an earlier new fill/finance entry and total change through the production
dispatcher's savepoint. Forged provenance cursor and unsupported projection
are rejected. The sealed stopped import is restored after the witness and
normal owner activation still denies, including this new page command.

Local Windows complete execution library: 258 PASS, 0 FAIL, 2 ignored (5.80s
after build). Direct page/import witness PASS (0.51s). Architecture/negative
self-tests and formatting PASS. RB01-RB16 and T09/T10 26/26 not accepted.

Next-stage draft requested by owner: GitHub issue #512, whole Rust T09/T10
acceptance after #510; not a replacement for current acceptance or activation.

---

## #510 - translate original rowid frontiers during unified import

The stopped importer now copies rows in original rowid order, then translates
mutable readiness frontiers for fills/bills/operation_receipts/finance_entries/
cash_flows and finance_provenance watermark to the same account-local source
prefix in target rowids. Original monetary facts, IDs and immutable receipts
are not rewritten. Nonpositive source rowids, target order mismatch or a
frontier outside original history reject and roll back the entire import.

Direct original two-account import regression now has identical financial
entry IDs and shifted B target rowids. It proves a partial source stays
partial, a fully sealed B stays sealed, reviewed prefix still refers to the
same event, and an impossible watermark rejects with no target tables left.
Full library import/crash/reopen regressions still pass.

The content digest uses r4-v2 and includes referenced rowids, since they now
carry persisted frontier semantics. Older OFFLINE candidate manifests are
not silently accepted under the new digest; isolated re-import is required.
No installed database/manifest was edited. This resolves the known mutable
frontier mapping blocker recorded in the preceding checkpoint, not the
remaining native activation/full page/capture and RB01-RB16 acceptance.

Local native Windows: complete execution library 258 PASS, 0 FAIL, 2 ignored
(6.16s after build); Agent all-target check PASS; architecture/negative tests,
format and diff PASS. No 26/26 or trading activation claimed.

---

## #510 - scoped cursor and provenance transaction operations

Original cursor/provenance callers now delegate to shared AccountStore SQL
operations with optional installation/account scope. Scoped monotonic cursor
updates use original generation/watermark rejection. Provenance reads and
advancement are account-scoped and preserve the original rule: a previously
unsealed non-empty history is not silently sealed by appending a new event.
No public cursor-only mutation or activation bypass is exposed.

Full imported-schema witness verifies A/B frontier isolation, generation and
watermark regression rejection, explicit TEST verified frontier then event
append/seal, unchanged B provenance, and rollback restoring event/frontier/
source cursor together. This is a composed transaction witness, not a complete
production unified-page handler. The positive starting seal is TEST SEEDED;
it is not an import audit or authenticated source certificate.

OPEN MIGRATION BLOCKER: original provenance/review receipts contain physical
SQLite rowid references. Full multi-account import reassigns rowids. Before
activation, the importer/verification must explicitly preserve or revalidate
the meaning of these references; equal integers cannot prove preserved
financial coverage. No activation may be authorized from the copied mark.
This pass does not claim to resolve that mapping.

Local Windows full execution library: 258 PASS, 0 FAIL, 2 ignored (6.25s after
build). Architecture/negative self-tests and formatting PASS. Full native
page/capture, cutover, nine actions and RB01-RB16/26x remain unaccepted.

---

## #510 - shared canonical financial event and totals transaction body

Original page ingestion now calls record_finance_entries_sql in the same
existing transaction. The helper supports explicit installation/account
scope for the unified original tables: canonical event replay/conflict,
reversal original/uniqueness, exact instance/account totals, source links,
monotonic USD-loss and daily-loss rows. No second calculation or commit is
introduced. Account campaign-loss update remains in the original page owner;
full unified page/provenance/cursor and capture wiring are not complete.

On the full original imported schema, direct checks verify same IDs on A/B,
independent -1 account/instance fee totals, exact replay, a second source
link without second charge, foreign-account reversal rejection, valid
correction replay and final fee total, USD loss 3 then replay 0, daily loss 3.
These source-domain witnesses roll back the sealed offline import, not an
accepted native activation. Existing original ingestion regressions continue
to exercise the production caller with real commits.

Local Windows: complete execution library 258 PASS, 0 FAIL, 2 ignored,
8.34s after build. Direct original-import witness PASS (0.39s). Architecture
and negative self-tests PASS. No complete-page/native activation/9-actions
or RB01-RB16/26x acceptance is claimed.

---

## #510 - account-scoped financial source verification

The original strategy finance provenance verifier now lives in
ledger/sql/source_finance.rs and is shared by existing account ingestion and
source checks on unified storage. Legacy callers still use their original
scope. Unified queries explicitly scope original reversal, fill, bill and
source-reuse lookups by installation/account. Rule codes and arithmetic
remain the same; no new financial authority or source projection is added.

Original imported-schema witness verifies local fill/bill positive cases,
foreign-account source denial, wrong fee amount/currency, same-account
source reuse denial, absence of false denial from another account's matching
source_ref, missing correction original and unknown account. Scoped checks
are not yet a complete unified financial page ingestion implementation.

Local Windows full execution library: 258 PASS, 0 FAIL, 2 ignored (6.24s
after build). Architecture guard/negative self-tests and rustfmt PASS.
Financial totals, atomic complete-page cursor and producer wiring remain
unfinished; no accepted activation, RB01-RB16 or 26/26 claim.

---

## #510 - original bill and order-link storage through unified owner

Original AccountStore ingestion and the unified RecordSourceBills dispatcher
share record_source_bills_sql. Raw bills and their immutable venue-order
links are scoped by installation/account in the unified original tables.
The owner retains the activation gate and same transaction/savepoint; these
facts alone do not advance coverage or change any financial total or grant.

Contracts on full original imported schema verify two accounts with identical
bill IDs, exact replay, conflicts in time/amount/currency/kind/economic event,
changed order link, missing bill, foreign-account bill, unknown account and
rollback of a preceding bill when a later order link is invalid. The stopped
owner rejects source writes. Raw-source assertions now live together in
account_raw_source_tests.rs instead of growing account_store_tests.rs.

Local native Windows: full execution library 258 PASS, 0 FAIL, 2 ignored
(6.31s after build); Agent all-target check PASS; architecture/negative
self-tests PASS. Full financial projection/cursor commit and production
source-capture wiring remain incomplete. No RB01-RB16/26x acceptance or
installed-state activation is claimed by this source-storage slice.

---

## #510 - shared immutable source-fill storage

Original ingest_page_observed and the unified RecordSourceFills dispatcher
now use one AccountStore::record_source_fills_sql implementation. It validates
positive quantity/price, overflow and immutable economic identity, writes the
original fills and fill_projection_pending tables, and scopes unified SQL by
installation/account. No coverage cursor, financial total or settlement grant
is changed by this operation. The public unified owner method uses the same
activation gate and single commit as other writes; stopped imports still deny.

Original imported-table witness: identical venue IDs on A/B remain separate;
exact replay yields two fills/two pending rows total; altered fee conflicts;
a later invalid fill rolls back an earlier fill via the original dispatcher
savepoint; foreign installation fails; generation remains unchanged. The
witness rolls back to preserve the sealed offline import. It does not claim
accepted activation or full source-page/producer wiring.

Local native Windows full execution regression: 258 PASS, 0 FAIL, 2 ignored,
6.51s after build. Direct original-import witness PASS (0.43s); architecture
and negative self-test PASS. Complete ingestion, source provenance/finance
projection and observer startup wiring remain outstanding before RB01-RB16.

---

## #510 - original account source cursors through unified finance owner

Production UnifiedFinanceHandle now reads source_generation_base(account)
and source_cursor(account, source, partition) from original source_cursors,
scoped by installation AND account. No separate account connection. Missing
account returns UNIFIED_SOURCE_ACCOUNT_NOT_FOUND instead of generation zero.
Original generation bounds are shared with the legacy AccountStore method.

The original import/owner witness now ingests A/B cursors with identical
source/partition and different generations (5/11) and values before import.
Unified reads preserve isolation, missing streams return None, foreign
accounts fail, and close/reopen retains original generations and cursor.

Local Windows: complete execution library 258 PASS, 0 FAIL, 2 ignored
(5.81s after build); Agent all-target cargo check PASS; architecture guard,
negative self-tests, formatting and diff checks PASS.
This prepares source producer wiring. Capture/ingestion, native activation,
9/9 actions and full RB01-RB16/T09/T10 are still incomplete.

---

## #510  BEGIN_POST uses original finance dispatcher in physical witnesses

The existing test-only causal input constructor now calls
`apply_original_unified_finance_transaction(UnifiedFinanceWrite::BeginPost)`
instead of bypassing the common command/savepoint boundary and calling the
RiskBook mutation directly. The same original positive/rejected/replay tests
and child-process BEFORE/AFTER COMMIT kill/reopen witnesses now traverse
that dispatcher. The causal input is still explicitly synthetic; this does
not prove authenticated live source construction or enable native POST.

Added a real SQLite failure boundary: a TEMP trigger aborts the original
account intent POSTING write after the RiskBook change. The public error is
RISK_STORAGE_UNAVAILABLE; original risk/account states and book sequence
must remain unchanged even if the outer caller commits after the error.
After trigger removal the existing successful/rollback/crash/replay path
still passes, so an always-denied implementation cannot satisfy the test.

Local Windows direct named test PASS (0.44s after build); full execution
regression 258 PASS, 0 FAIL, 2 ignored (6.60s), architecture self-test/guard,
format and diff PASS. Counts overlap. Complete RB0116/T09T10 and real
source/activation remain unaccepted. Installed data and runtime untouched.

---

## #510  complete committed entry-budget readback (15 Nov 2026)

`UnifiedFinanceHandle::campaign_summary` now preserves the original
RiskBook entry limit, consumed amount, reserved amount and bound breach,
in addition to the existing remaining amount. These values come from the
same original verified readback and SQL snapshot, without reimplementing
money arithmetic in the Agent/inspection layer. The stopped inspection's
existing serialized campaign view exposes these fields automatically.
Opening provenance/time and live authorization are NOT fabricated.

The original two-account imported-owner test independently checks exact
100 limit / 0 consumed / 2 reserved / 98 remaining / 0 breach, serialized
values, refusal of a foreign campaign, and unchanged repeated readback.
Local Windows named test PASS (0.37s after build). Full contract and Agent
checks are recorded in the associated implementation report. This change
completes that read projection, not production mutation/activation or 26/26.

---

## #510  explicit unified finance responsibilities (15 Nov 2026)

The installation financial owner was moved out of account_worker.rs into
its private unified subtree. The existing public account_worker API remains
an explicit re-export; the same Connection, FIFO, transaction dispatcher,
authority gate and original receipt readers are used. No new backend, writer,
activation mode or monetary algorithm was introduced.

Owner lookup:
- unified/mod.rs: command/result/readback types and narrow facade (240 lines);
- unified/owner.rs: queue, connection and shutdown lifetime (594, including queue tests);
- unified/readback.rs: committed-state queries (209);
- unified/sources.rs: original SQL receipt validation (264);
- unified/transaction.rs: atomic command dispatch and activation gate (234).

Local Windows regression: full execution library 258 PASS, 0 FAIL,
2 ignored, execution 6.12s after build. Architecture self-test/guard,
rustfmt and diff checks PASS. Ignored tests are not proof. Agent all-target
cargo check is recorded separately in the accompanying implementation report.
Parent bf4bf2d0 also passed canonical Draft CI 43673703720; full Windows
T09/T10 was SKIPPED, not accepted. This restructuring does not close RB0116
or authorize activation; source/9-action completion and RB09 remain required.

---

## #510  settlement domain extraction and diagnostic reason preservation (15 Nov 2026)

The production SQL settlement transaction now calls
`okx_kernel::risk::settlement::calculate_entry_settlement` for cumulative entry
consumption, remaining reserve and the monotonic bound-breach increment.
The previous arithmetic body and overage helpers were removed from the SQL
module; source validation, revision checks and atomic writes remain inside
its original transaction. Independent fixed expected values cover partial
settlement, excess cost, late fees after release, unrelated reservations,
corrupt reserve and cumulative regression.

`RiskRuleError::code` and `MoneyError::code` preserve specific causes through
`original_finance_error` instead of collapsing them to RISK_RULE_DENIED or
RISK_MONEY_INVALID. A direct boundary regression asserts the exact causes.

Local native Windows, Rust 1.95.0, debug profile, on parent `5c793735`:
- direct `risk_book_contract`: execution 80 PASS (2.12 s), kernel 8 PASS;
- complete execution library: 258 PASS, 0 failed, 2 ignored (5.80 s);
- kernel all-target Clippy with -D warnings, architecture guard/self-test,
  rustfmt and diff checks PASS.
Counts overlap; ignored tests are not accepted. These are individual measured
execution times after build, not P95 or complete RB0116/26/26 acceptance.
The installed system and activation gate were not changed. Remaining work:
production source/9-action completion, actual verified activation/legacy stop,
module separation in account_worker/store, final Windows acceptance and P95.

---

## #510 â close maintenance bypass on authority-mode gate

Admin `InitializeAccount`, `BackupAccount` and `RestoreAccount`
are stopped-Engine commands, but previously the restore/backup code could
call original `AccountStore::open_existing` directly, bypassing the shared
mode lease already used by the regular `AccountStoreHandle`. New
`hold_legacy_account_maintenance` reuses the EXACT existing installation
`authority-mode.lock`, holds a shared OS lease during the FULL stopped
administrative operation, checks canonical installation/account identity,
and refuses while native finance.sqlite is present. The native owner needs
an exclusive lease and therefore cannot enter between an administrative
preflight check and the backup/restore's SQL/file writes. This adds **no**
Cloudflare state, no extra SQL owner, no daemon and no bespoke fencing.

Direct native test proves shared maintenance excludes the unified writer,
finance.sqlite import refuses legacy access even without a live owner,
and a foreign root cannot borrow another account's maintenance lock.

---

## #510 â runtime owner selection (actual Run/Once startup)

The canonical `okx-agent Run/Once` path now checks for the installed
`runtime_root/finance/finance.sqlite` BEFORE opening any account resources.
When the unified file exists it **does not** instantiate the former
`AccountRuntimeRegistry::open`, account-local `AccountStoreHandle`,
private source observers, source capture loops or Cloudflare financial
`NativeRiskClient::new`. Instead the same production `TradeService` is
constructed with `NativeRiskClient::local_stopped` backed by the original
single `UnifiedFinanceHandle::open_stopped_import` owner.

To open this strictly read-only installed mode the operator MUST supply
`--stopped-finance-campaign <exact-campaign-id>`. The independent existing
Protobuf `account-catalog.pb` supplies the complete exact REAL account
roster. Original manifest/RiskBook verify the full one-campaign inventory.
Missing/mismatching roster, missing/extra campaigns, or any invalid import
fail before the Engine session starts. Neither missing finance.sqlite nor
missing campaign can silently fall back to Cloudflare finance. The native
registry denies `ConnectAccount`, auto-recovery, private account contexts
and every venue trade. The local single-writer finance owner is held for
the Engine lifetime and its exclusive lease excludes old SQL writers.

Authenticated governance snapshots may be loaded into memory **for
observations only** if their full roster, revision, policy cap and
per-account paused/revision matches the ORIGINAL scoped SQL rows read from
the same installation writer. No incoming governance is committed or
promoted from `OFFLINE_NOT_AUTHORIZED`. `Summary`, `AccountSummary`
and `Inspect` remain source read-only; all six mutations are denied
before any COMMIT. This is the FIRST REAL native branch in existing
Run/Once production startup rather than isolated test/CLI handlers; it
does NOT make trade/POST production-capable or constitute RB09.

Outstanding: migrate original account capture/quote/fill/bill/fee,
ledger/protection/recovery consumers into the unified writer; finish four
remaining mutation ingress actions; verified RB09 old-Cloudflare financial
shutdown, immutable activation and exact-SHA Windows RB01â16/T09/T10/26x.
No merge, deploy or real/demo trade authorized.

---

## #510 â source-bound native ReleaseUnsent

Native `RiskCommandAction::ReleaseUnsent` is now mapped to the existing sole
`UnifiedFinanceHandle` financial transaction (never to Cloudflare in local
mode). The untrusted wire's no-send proof is matched against the ORIGINAL
AccountStore `REJECTED_UNSENT`, pre-POST state, durable reserve-attempt
receipt and precise campaign/account/intent/plan/request hash. The original
`UnsentSqlProof::from_account_receipt` canonical JSON SHA-256 is checked;
no caller flag can mint a no-send fact. The ORIGINAL RiskBook native
`release_unsent_unified_native` then independently reads that same original
source and applies the release or no-reserve fence with ONE SQLite COMMIT.
The exact output maps to `RiskResult::Updated`; never a venue POST grant.

Direct SQL tests prove a genuine no-send address; foreign account/campaign,
altered plan, reason or record digest are all rejected. Exact original SQL
reader is reused; no duplicate financial/no-send state machine is built.
Current import's `OFFLINE_NOT_AUTHORIZED` still denies actual financial
COMMIT before mutation. Source producers for original AccountStore remain
an outstanding part of complete production cutover, as does RB09.

---

## #510 â original AccountStore source-bound SettleEntry ingress

The canonical native RiskCommand `SettleEntry` now forwards its exact
reservation/plan/sequence/amount/revision/source-digest comparison fields to
`UnifiedFinanceHandle::settle_bound_entry`. This is not a separate financial
backend: its existing `apply_financial_write` caller obtains the sole
installation-owned `BEGIN IMMEDIATE` transaction, checks RB09 authority,
reads original `risk_entries` account/intent/generation and the exact original
immutable AccountStore partial, terminal and late receipt. The wire's USD
amount **cannot** debit the RiskBook unless it matches the ORIGINAL SQL
receipt's money, revision, plan, reservation and source digest; ambiguous,
missing, foreign, stale or changed source fails closed. Only then the original
`risk::settle_entry_unified_native` mutates the RiskBook and AccountStore ACK
in the SAME COMMIT. The exact output is mapped into `RiskResult::Updated`,
never `Held` or `PostAuthorized`.

A direct integration witness reuses the original AccountStore scoped
partial fill/bill receipt SQL: exact amount 0.4, revision 1, plan and digest
select the real `Partial` producer; changed money, plan, revision, digest or
foreign account cannot select one. The original terminal/late SQL producer
and single mutation algorithm are reused unchanged. **With the current
OFFLINE_NOT_AUTHORIZED import the live route is still denied BEFORE it can
COMMIT.** This is substantive native wiring of one mutation, NOT acceptance
of 9/9, no replacement for still-unmigrated fill/bill source ingestion or
independent RB09 server-disable/activation evidence.

---

## #510 r4.1 â native whole-cohort governance route

`NativeFinanceRisk::apply_governance` converts an already validated,
installation/campaign-bound signed `CentralGovernanceSnapshot` into a
sorted, full native REAL-account policy roster. It denies unsupported mixed
Demo cohorts; it never silently drops a policy account. The existing
`TradeService::update_governance` chooses the native policy path when its
`NativeRiskClient` is bound to the local finance SQL owner and returns
immediately on either success or failure: **no per-account legacy writer
fallback after a native rejection**. That path uses the already implemented
`AccountStore::apply_unified_campaign_policy_sql_transaction` to change
every original `account_policy` row plus RiskBook policy revision under ONE
`BEGIN IMMEDIATE/COMMIT`. The still-unmigrated legacy startup retains its
historical per-account path and is NOT run in parallel with the native owner.

An offline test now exercises the SUCCESSFUL original whole-cohort policy
domain transaction while always ROLLING BACK, alongside missing-cohort and
unproven-ceiling negative cases. RB09 activation and production account source
migration are still required: this is a source-wiring step, not final 9/9
or enabled Real/Demo trading.

---

## #510 r4.1 â local RiskCommand ingress, original SQL readback

The existing `NativeRiskClient` now has an explicit local Rust route type
alongside the legacy WebSocket transport. A local request never queues a
Cloudflare risk command. The adapter handles the canonical nine
`RiskCommandAction` variants exhaustively: `Summary`, `AccountSummary`
and `Inspect` read **original** RiskBook SQL through the existing sole
bounded `UnifiedFinanceHandle`, preserving account/campaign/plan scope,
current sequence and original source activity time. An absent exact-plan
reservation is DENIED, never safe-to-send. Both risk and account summaries
are built from verified original projections (no new budget arithmetic).
`InstallEntryOpening`, `Reserve`, `SettleEntry`, `ReleaseUnsent`,
`BeginPost` and `Resolve` return the explicit
`FINANCE_CUTOVER_NOT_ACCEPTED` gate. The local ingress is not a new
financial COMMIT owner.

This is **NOT** production nine-action cutover: the unaccepted source
producers and staged startup still use the legacy account-local
`AccountStoreHandle`; the native routing mode must not be selected by
production startup until original AccountStore actions are all scoped,
RB09 server finance mutation is disabled and the independently verified
activation receipt exists. All old Cloudflare finance logic is preserved
*only for the unswitched legacy install*. No fake 9/9 acceptance, no real/demo
trading.

---

## #510 r4.1 â unified original AccountStore+RiskBook policy CAS

New `UnifiedFinanceWrite::ApplyPolicy` is part of the existing original
Rust financial transaction dispatcher. `UnifiedFinanceHandle::apply_governance_policy`
does not create another SQL thread, connection or per-account COMMIT.
Original `AccountStore::apply_unified_campaign_policy_sql_transaction`
verifies the **full** registered account roster and every original scoped
`account_policy` row, campaign identity/immutable limit/revision, and
updates every account plus `risk_meta.policy_revision` under the SAME
financial `BEGIN IMMEDIATE/COMMIT`. A partial roster, stale/contradictory
replay or unproven financial ceiling change fails closed.

The actual `apply_governance_policy` endpoint is still denied on an
`OFFLINE_NOT_AUTHORIZED` import. Offline tests invoke the **same**
transaction dispatcher with mandatory rollback and negative cohort/limit
witnesses. `TradeService::update_governance` still uses legacy per-account
owners before cutover; it is NOT migrated just because the new transactional
source operation exists. A complete account-source/9-action production switch
and RB09 server-disable/import/activation still block GO.

---

## #510 r4.1 â native stopped-finance inspection (read-only, no trade owner)

A built-in `okx-agent inspect-finance` command reads an existing, independently
rostered and *sealed* 47-table stopped import through the ORIGINAL bounded
`UnifiedFinanceHandle` SQL worker. It is selected **before** the legacy
`AccountRuntimeRegistry` and before market/Cloudflare/venue initialization.
No account-specific writer, legacy financial RPC or venue POST is started.

Input: explicit `--installation-id`, one or more `--expected-account`
and `--expected-campaign` values from an independent stopped-source roster,
and `--campaign` (plus optional `--account`, or the exact pair
`--reservation-key`/`--plan-sha256`). The import validator checks the
roster, source primary rows, manifest digest, FKs and original scopes;
a missing/invalid/staged-only source rejects, rather than producing a
fictional empty risk budget. Results contain the original campaign,
account, reservation readbacks, `trade_authorized:false` and immutable
`OFFLINE_NOT_AUTHORIZED` state. A missing reservation is **UNKNOWN**,
not POST permission. The worker lease is observed released before exit.

**Acceptance scope:** only the three *read* shapes (Summary,
AccountSummary, Inspect) have an external native stopped-import diagnostic
consumer. The remaining six mutation commands and ALL production trading
remain on the unaccepted cutover boundary; this command is not an active
TradeService route and does **not** constitute 3/9 production cutover.
RB09 server-side legacy finance disable, source producer verification,
nine-action production wiring, Windows physical 26/26 remain NO-GO.

---

## #510 r4.1 â unified finance queue admission and SQL outcome boundary (candidate)

The one installation-wide `UnifiedFinanceHandle` now distinguishes definite
**pre-acceptance** failures (`UNIFIED_FINANCE_QUEUE_FULL`,
`UNIFIED_FINANCE_WORKER_STOPPED`, expired start deadline) from a dropped SQL
reply **after enqueue** (`UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY`).
The production-shaped write entrypoint has a bounded **start deadline** only;
it does not cancel an already-started SQL commit, nor grant venue replay.
SQLite COMMIT uncertainty is likewise an unknown outcome requiring original
identity/receipt readback. The original `OFFLINE_NOT_AUTHORIZED` production
gate remains active; no financial mutation was enabled by this change.

Native deterministic direct tests: rejection before enqueue leaves the FIFO
empty; a full or disconnected queue refuses before acceptance; a worker that
drops an accepted reply returns UNKNOWN; a single real SQLite worker preserves
interleaved A/B/A account ordering. These are queue-boundary witnesses, **not**
the full RB15 load/fairness/backup or Windows 26/26 acceptance. Exact SHA CI
and final production cutover remain required.

---

# Trade acceptance evidence map â #448 / #449 / #501

## #510 â original finance domain dispatch and legacy-route hard stop (candidate)

**Proof target on the exact candidate SHA, pending CI:** the original
AccountStore/RiskBook native operations now share one typed transaction
dispatcher (`apply_original_unified_finance_transaction`) for reserve,
begin-post, source-backed partial settlement, no-send release, outcome
recording and account/installation recovery. The old test-only
Reserve/Partial/Unsent rule wrappers were deleted. The existing
two-account same-ID original SQL witnesses now execute those same
domain entrypoints under rollback and preserve the imported original
47-table source.

**Mandatory negative:** `UnifiedFinanceHandle::apply_financial_write`
is denied `FINANCE_CUTOVER_NOT_ACCEPTED` on a verified
`OFFLINE_NOT_AUTHORIZED` import even for posting recovery and outcome,
with identical inventory/manifest after refusal. No grant, COMMIT or
second writer may follow. Independently,
`accounts::tests::finance_staged_import_never_falls_back_to_old_trade_or_cloudflare_finance`
must prove a zero-account old registry refuses a staged finance DB;
this test is included in the existing Draft Rust CI job, not a new
workflow.

**NOT_PROVEN remains:** authenticated production risk source,
`InstallEntryOpening`, complete `Resolve`, terminal and late fee
settlement under one COMMIT, all nine TradeService routes, RB09 server
finance-disable and verified imported-state activation, physical
Windows RB01â16/T09/T10 26/26, native direct P95. No old
Cloudflare finance mutators are disabled yet. Do not count this
test-passed candidate as completion.

---


## #510 r4.1 â RB09 one-time stopped legacy-finance transition; NOT a continuous Cloudflare gate

**Current acceptance definition supersedes previous owner-epoch/fencing wording below; historical SHA-specific records are preserved as historical evidence.** The new Rust financial path never calls Cloudflare to reserve funds, grant POST, settle, release or check a permanent owner lease. Cloudflare MCP, authorization and routing remain.

**RB09 acceptance table (isolated infrastructure, not an installed-environment migration):**

| Boundary | Positive expected | Negative expected / hard stop |
|---|---|---|
| Stop/disable previous finance authority | Old executors stopped; old Cloudflare financial mutators/POST grants return explicit refusal, including after allowed restart/rollback; MCP/auth/transport continue | Any accepted old financial mutation â NO-GO |
| Final source census | Exact installation/account/campaign roster; reserves, consumption, fees, revisions, UNKNOWN and receipts reconciled | Unknown source, missing snapshot, hidden obligations, unproven empty state â NO-GO |
| Empty-state fast path | Evidence of no historical financial obligation permits empty unified SQLite bootstrap | Absence inferred merely from non-production use or missing files â NO-GO |
| Nonempty-state import | Original facts/IDs/budgets/receipts preserved under one SQLite COMMIT, verified on reopen | Incomplete or corrupt transfer, wrong identities, altered totals â NO-GO |
| Kill / lost import reply | Pre-COMMIT no partial target authority; post-COMMIT all old obligations present; exact retry does not mint headroom | Two live financial owners or extra reserve after retry â NO-GO |
| New Rust activation | Production finance writer becomes available only after stop + verified snapshot + exact import/EMPTY proof | `OFFLINE_NOT_AUTHORIZED` or any unproved gate â NO-GO |

**Evidence format:** exact SHA, legacy endpoint negative mutation request/response and final readback, source roster/digests/financial totals, target SQLite receipt, before/after-kill reopen, replay, error diagnostics. RB09 is separate from the fast Rust direct target; do not repeat the 90-kill historical Native/Worker matrix. Record **NOT_PROVEN** while old Cloudflare financial code still accepts operations in the deployed legacy environment. Actual installed migration will be executed separately after code acceptance.

---
## #510 r4.1 â installation authority mode OS lock (Draft / exact CI pending)

**New native architecture check:** `finance_owner_gate.rs` owns the single
`runtime_root/finance/authority-mode.lock` file; the original
`AccountStoreHandle` and `RiskBookStore` open their canonical
account/campaign DB while holding SHARED installation-mode leases.
`UnifiedFinanceHandle::open_stopped_import` holds an EXCLUSIVE
installation-mode lease and its existing SQLite writer lock.
The old account closure witness releases its shared mode lease only
AFTER the SQL writer and outstanding backup have completed; RiskBook
`close` releases original SQL lock and then mode. Existing
`finance.sqlite`, including `OFFLINE_NOT_AUTHORIZED`, blocks a fresh
legacy writer. All failures are immediate, fail-closed, with no sleep,
retry, implicit promotion, or Cloudflare RPC.

**Physical native tests:** two simultaneous old shared leases versus
exclusive new; exclusive new versus old; staging finance.sqlite
refuses old; actual `AccountStoreHandle::initialize_flat` and
`RiskBookStore::open_existing` use the same gate and preserve shutdown
ordering. Native child test obtains the old shared OS lock then parent
kills it and immediately proves the exclusive lock can open. These
are tests of **the new Rust version's local authority-mode exclusion**,
not an assertion that old deployed Cloudflare or an older legacy native
binary honors this lock. RB09 server-disable and exact source
import/empty proof remain separate one-time physical checks, and the
installed user environment is NOT migrated here. Draft CI / Windows
26/26 acceptance are still required.

## #510 r4.1 â installation-wide POSTING recovery under one SQL writer (unverified until CI)

**Code:** `AccountStore::recover_installation_posting_sql_transaction`
selects every account from the original scope-keyed `account_meta`
inventory and applies fail-closed source-checked recovery to all in
ONE caller-owned transaction. A nested native SQLite SAVEPOINT rolls
back prior account mutations on a later account error before the caller
returns; it does not introduce another DB or independent COMMIT.
The single `UnifiedFinanceHandle` FIFO exposes a test-only,
always-rollback installation recovery trial on an offline import.

**Direct physical witness:** extended same-ID A/B full 47-table
`risk_book_contract_unified_original_prepare_is_scoped_atomic_and_idempotent`
creates two new native original intents and POSTED receipts, makes B's
source contradictory after A has staged, verifies A and its receipt
already reverted by the SAVEPOINT, then commits the successful two-account
batch exactly once, replays as zero and reopens original WAL to prove
both `UNKNOWN` with previous terminal intents intact. The full
source-import owner test also exercises rollback-only roster recovery.
This remains Draft/NO-GO pending exact SHA CI; no real venue or
Cloudflare authority action has been undertaken.

## #510 r4.1 â one-owner recovery source and scoped readback regression (unverified until CI)

**Primary production-symbol boundary:** the original
`AccountStore::recover_posting_sql_transaction` now requires real
original `POSTING` fence + source receipt prior to applying
`POSTINGâUNKNOWN`. Receipt `POSTEDâUNKNOWN` and intent change
are one scoped transaction. Existing `UNKNOWN` or terminal receipts
cannot be silently overwritten by another recovery.

**Direct SQL witness:** the existing
`risk_book_contract_unified_original_prepare_is_scoped_atomic_and_idempotent`
checks full 47-table schema, two same-ID accounts, negative missing
receipt, altered receipt state/plan/revision, invalid durable
`begin_post_at`, cross-account exclusion, no-op recovery replay,
and subsequent original outcome/terminal receipt state.

**One installation writer witness:** the existing
`risk_book_contract_one_installation_finance_owner_reuses_original_import_and_lock`
now reads original intent/plan through
`UnifiedFinanceHandle::account_intent` and invokes test-only
scoped recovery and outcome in the same bounded owner. These
operations always roll back and preserve the verified 39+8
offline-source checksum. This is NOT authenticated source readback,
a production Rust financial grant, an actual Cloudflare stop or
Windows 26/26 acceptance. Post-run exact SHA/CI is required.

## #510 r4 â original combined Reserve passes through ONE installation FIFO, strictly rollback-only (14 Nov 2026)

**Verified code SHA `8b4d177b6f7f9ae798953768616c0da23ea73813`**, [Draft CI #43638259025](https://github.com/iamaman11/okx/actions/runs/43638259025) **SUCCESS**: architecture guard and rustfmt PASS, direct RiskBook 72/72, kernel 6/6, original AccountStore source 1/1, complete `okx-execution --lib` **242 PASS / 0 FAIL / 3 ignored**; the direct selections overlap the complete library run, not unique extra cases. Windows RB01âRB16, T09/T10 26/26 and Cloudflare remote fencing jobs are SKIPPED, **NOT ACCEPTED**.

**Precise new evidence:** `UnifiedFinanceHandle::trial_original_reserve` is compiled **only under `#[cfg(test)]`**. It queues the same full original `RiskBookStore::reserve_unified_account_transaction` over the installation's one bounded SQL owner/Connection, executing `BEGIN IMMEDIATE`, invoking the original AccountStore + RiskBook checked rules and a SAVEPOINT, and **ALWAYS `ROLLBACK`** even if an internally staged `HELD` was returned. It verifies `OFFLINE_NOT_AUTHORIZED` before staging; no receipt, spending, or permission to call an exchange can escape. A test constructs two genuine original account v2 source stores, the original v5 campaign SQL fixture with matched original manual strategy policy (fixture-only normalization), imports the complete 39+8 original tables into one manifest-verified finance.sqlite, observes a test-only successful combined HELD trial, rejects strategy-bucket substitution using the original owner rule, verifies unchanged campaign summary, and cold-reopens with the **original immutable import checksum still valid**. The existing standalone original combined Reserve tests already prove single COMMIT/OS-kill/replay in raw scoped SQL, but this new bounded-queue test is deliberately NOT a durable Reserve cutover.

**Why rollback-only:** The durable `finance_import_manifest` includes an immutable content SHA-256 of *all 47 original source financial tables*. Any real post-import write before a formally verified authority/epoch transition would invalidate the cold-reopen proof and could create a second writer/finance authority. Merely changing `OFFLINE_NOT_AUTHORIZED` or forging a test census must never enable trading. Complete trusted source provenance, old Cloudflare owner server-side revocation and epoch fencing, active durable recovery semantics and all nine real TradeService action callers remain **NOT PROVEN**. PR stays Draft/NO-GO; no main merge, Demo/REAL or installed Windows change.

## #510 r4 â original scoped Summary / AccountSummary / Inspect via ONE offline finance owner (14 Nov 2026)

**Verified CODE SHA `22e7f7f2ef13b784ec1f79c6a1637effb332cd71`**, [Draft CI #43636557320](https://github.com/iamaman11/okx/actions/runs/43636557320) **SUCCESS**: architecture and fmt PASS; direct RiskBook 72/72, kernel 6/6, original AccountStore source 1/1; full execution lib **242 PASS / 0 FAIL / 3 ignored**. The direct selections overlap the full-library suite, not extra unique tests.

**One original source for observations:** `risk::book::store::queries::{account_readback,inspect}` now accept optional `(installation_id,campaign_id)` scope. Legacy v5 callers still pass None and preserve original query/rule semantics; v6 readback requires composite scope for original policy/occupied/loss/entries/no-reserve rows, checks original campaign aggregates, verifies native canonical account identity, and returns the original typed `AccountReadback` / `ReservationReadback`. `UnifiedFinanceHandle` enqueues typed `campaign_summary`, `account_summary` and `inspect_reservation` through **the same bounded FIFO/Connection**, with no second SQL connection/writer, formula or new financial authority. Wrong campaign fails closed, wrong immutable plan/missing receipt returns None (**UNKNOWN, not no-send permission**). Two real original account sources plus native RiskBook source validate two accounts and their distinct HELD/no-reserve identities; separate two-campaign original import proves same historical reservation key is not leaked between X/Y where original plan hashes differ.

**Scope and acceptance:** These are *three private read-only implementations*, **NOT 3/9 production RiskCommandAction cutovers**. OFFLINE_NOT_AUTHORIZED import marker remains; no trusted producer/census/epoch, venue/Cloudflare authority, original terminal/late write routes, nine real TradeService callers, or installed Windows RB01âRB16 / T09/T10 26/26 acceptance. Continue with original unified SQL mutation routes only behind verified cutover/fencing; keep PR Draft/NO-GO, no main merge or trading.

### Subsequent exact-source campaign/epoch guard â verified on final Reserve code

**Code SHA `47b53099a442b10002a2e6a7eef76769ef0e7771`**, [Draft CI #43632385416](https://github.com/iamaman11/okx/actions/runs/43632385416) **SUCCESS**: 69 execution + 6 kernel + 1 original AccountStore direct, full execution library 239 passed/0 failed/3 ignored, rustfmt/architecture PASS. Before RiskBook can stage a scoped reserve, `RiskBookStore::reserve_unified_account_transaction` now requires same-file original `intents.risk_increase=1` and matching `account_policy.campaign_id`, `account_policy.revision`, `paused=0` under the SAME SQLite transaction. Existing original RiskBook capacity and legacy per-campaign behavior are unchanged. Negative SQL witness mutates campaign, revision, pause, or risk-increase independently and checks fail-closed `UNIFIED_ACCOUNT_CAMPAIGN_POLICY_UNPROVEN` without budget changes. This is test-only v6 finance source, **not** authenticated production cutover or Cloudflare fencing.

## 14 Nov 2026 â #510 r4 ORIGINAL Reserve + RiskBook in one PRIVATE SQLite transaction (Draft PASS)

**Exact source SHA `2395bb7aa8600234890d9e3354b83ac303edb004`**, [Draft CI #43631998537](https://github.com/iamaman11/okx/actions/runs/43631998537) **SUCCESS**: `riskbook-draft-rust` performed architecture guard/negative self-test and rustfmt PASS; **69/69** direct `okx-execution` RiskBook cases, **6/6** `okx-kernel`, **1/1** original AccountStore source, complete `cargo test --locked -p okx-execution --lib` **239 passed, 0 failed, 3 ignored**. Three ignored tests are not accepted. Full Windows, Cloudflare and T09/T10 jobs were **SKIPPED** on Draft; this is **not** RB01â16 / 26/26 acceptance.

**One original Rust source for each operation:** Existing `AccountStore::reserve` delegates to `AccountStore::reserve_sql_transaction(&Transaction, scope, receipt, now)`; original plan, decision owner, intent state, account identity, idempotency and exact `risk_receipts` are checked **inside** that transaction. Existing `RiskBookStore::reserve` delegates to `RiskBookStore::reserve_sql_transaction(&Transaction, scope, input, census)` with the original capacity and monotonic source-loss rules, including fully scoped installation/campaign SQL. `ReserveDecision::Rejected` is a typed **ordinary budget outcome**, not an automatic rollback: caller must commit verified high-water without creating a new account reservation. SQL/source errors require rollback.

**Private joint operation:** `RiskBookStore::reserve_unified_account_transaction` calls these **two existing owners** under the *same caller-supplied SQLite Transaction*, without new Connection, queue, worker, money formula or independent COMMIT. It verifies account/reservation/plan/source identity. Original AccountStore owner mismatch after a staged RiskBook HELD fails the transaction; the caller rolls back **both** original states. Never treat the internal staged HELD as production venue permission.

**Executed proof:** `risk_book_contract_scoped_original_account_reserve_is_atomic_and_replay_exact` imports **two genuine original v2 AccountStore** source databases to a single offline v6 SQLite and proves scoped original receipt rollback/commit/reopen/exact replay/wrong-account conflict. `risk_book_contract_scoped_original_reserve_shared_budget_and_high_water_on_denial` builds *test-only* schema-backed original account/campaign rows and calls the actual joint Rust operation: A reserves 6/10, B attempts 6 and is denied, B's independently observed source-loss 1 persists across cold reopen, B stays PREPARED, A's one HELD+original receipt replays exactly, and a mismatching original decision owner forces full rollback. This second fixture is **not** authenticated production source provenance or a test of live TradeService.

**NO-GO:** `AccountStoreHandle` still opens per-account and `RiskBookHandle` per-campaign; no one real installation-wide production SQL owner or trusted native producer. Missing complete authenticated census/owner epoch/Cloudflare server-side old-owner revoke, other RiskCommandAction and terminal/late/UNKNOWN/recovery production routes, real combined Reserve OS kill, installed Windows RB01â16 and T09/T10 26/26. Import manifest remains `OFFLINE_NOT_AUTHORIZED`. PR #511 stays Draft; no merge, deploy, Demo/REAL or installed Windows changes.

## Full original execution regression after shared BEGIN_POST â 237/237 PASS (14 Nov 2026)

**Code + draft CI SHA `70039cb06cd0f6581ebfe3d2f69cb05f211a7b94`**, [GitHub CI #43626550682](https://github.com/iamaman11/okx/actions/runs/43626550682) **SUCCESS**. Same EXISTING Draft RiskBook job now runs fast original/source/architecture/format guards **first**, then `cargo test --locked -p okx-execution --lib` using the same cached compilation; complete existing `okx-execution` library regression **237 passed, 0 failed, 3 ignored** (~15.06s after compile). RiskBook filtered cases 67/67 execution + 6/6 kernel + original source 1/1 still PASS. NOT Windows installed 26/26 or physical Cloudflare fencing; ignored tests are NOT claimed as passed. No second workflow/CI or new application engine. The single normal `riskbook-draft-rust` job remains a Draft code gate, not final acceptance.


## Latest #510 r4 â original AccountStore full BEGIN_POST checks + RiskBook single-commit and durable readback (14 Nov 2026)

**Verified CODE SHA `6a1db60a17ba7f3a62561557a58dcb9c32a3d8c8`** in [GitHub CI #43626228175](https://github.com/iamaman11/okx/actions/runs/43626228175) **SUCCESS**: 67/67 filtered `okx-execution` risk contracts + 6/6 `okx-kernel` + 1/1 original AccountStore source = **74 direct cases**; architecture guard/negative self-checks and rustfmt PASS. Filtered execution times post-build 1.15 + 0 + 0.11s Linux. **NOT** full native Windows RB01âRB16 or T09/T10 26/26.

**Source ownership (one function, not duplicated):** Original production `AccountStore::begin_post` now calls `AccountStore::begin_post_sql_transaction(&Transaction, scope, intent_id, policy_revision, now, required_reservation)`, the **same** validation and original receipt writer used for old v2 unscoped and new v6 `(installation_id,account_id)` SQL. Preserves original expiration, policy revision, paused/stops, strategy identity, storage-reconcile and unknown-exposure guards; now checks exactly one CAS for intent `RESERVEDâPOSTING` with `begin_post_at` and the *original* `risk_receipts HELDâPOSTED`. Scoped v6 additionally demands same original reservation identity and plan hash in the primary AccountStore receipt. This does **not** invent a new account owner or fee/ledger writer.

**RiskBook composition:** Private `risk::book::store::post::begin_unified_post_and_account_transaction` stages the ORIGINAL native RiskBook `HELDâPOSTED` risk/campaign checks, then invokes that original AccountStore method under **the same caller-owned SQLite Transaction**. Any failure is an error and the caller must roll back. Ordinary risk budget denial remains a distinct `PostDecision::Denied` (may persist a properly verified high-water source observation). Native `PostDecision::Authorized` remains *uncommitted internal state*, NOT venue-send permission and **not wired into production TradeService**.

**Original source, not handwritten test status:** The same two-account/two-campaign test now prepares and reserves through genuine original v2 `AccountStore::prepare` + `AccountStore::reserve` before importing 39+8 primary tables; it never manually writes `POSTING`. Negative scenarios: removed original `risk_receipts`, paused source, stale policy revision, expired deadline, incomplete storage recovery all reject. Real Rust child OS-kill before/after combined risk+original-account commit proves all-or-nothing native SQLite/WAL; cold reopen invokes ORIGINAL `AccountStore::post_fence_evidence_sql` to verify exact `begin_post_at=10002` and original POSTED receipt; wrong account/reservation rejects; RiskBook X vs Y isolation and no duplicate grant on replay preserved. Test causal `PostCausalProof` stays explicitly `#[cfg(test)]` synthetic.

**Still blocked:** Actual installed single bounded finance worker, trusted authenticated complete source census and Cloudflare server-side old-owner revoke/owner epoch, all nine production `TradeService` RPC/actions, real venue/replay recovery, partial/terminal/late fees, UNKNOWN and installed Windows RB01â16 / T09/T10 26/26. `finance_import_manifest` still `OFFLINE_NOT_AUTHORIZED`; old live writers untouched. PR Draft/NO-GO; no merge/deploy/live or Demo trading.


## Previous #510 r4: staged RiskBook BEGIN_POST with basic original AccountStore SQL â 74 direct tests (historical)

**Verified CODE SHA `ac3cf6ef9b8f09831e9504e8ab142562785c69b6`**, [GitHub CI #43623705885](https://github.com/iamaman11/okx/actions/runs/43623705885) **SUCCESS**: 67/67 `okx-execution` filtered risk contracts, 6/6 kernel, 1/1 original AccountStore fill/bill producer = **74 direct tests**; existing architecture guard/negative guard self-tests and rustfmt PASS. Linux filtered test execution ~0.99+0.00+0.07s after compilation, NOT installed Windows/RB01â16/T09/T10 26/26.

**One ORIGINAL native RiskBook `BEGIN_POST` algorithm now has two SQL transaction backends, without duplicating Rust money rules.** Legacy v5 `RiskBookStore::begin_post` still commits on its owner; the same `begin_post_transaction(&Transaction, request, scope)` can operate in the caller's scoped v6 `(installation_id,campaign_id)` finance transaction. Original `apply_census_loss` high-water implementation is reused with explicit campaign scope. V6 entry CAS, cohort account policy/occupied JOIN, campaign sequence CAS and loss source writes are ALL scoped. The v6 path also checks the **original AccountStore `intents` plan hash, `RESERVED` state, `risk_increase=1` and `account_policy.campaign_id`** inside the same transaction before any POST transition. Legacy PostCausalProof/CAS/freshness checks are not weakened. The staged native `PostDecision::Authorized` is NOT an external venue POST permit; no production adapter exists and test-synthetic causal input is compiled only with `#[cfg(test)]`.

**Real integration and OS-kill witness:** two genuine v2 AccountStore sources + two original v5 campaign RiskBooks import into one v6 SQLite. An original account without RESERVED/campaign policy is refused; one staged `HELD â POSTED` and account `RESERVED â POSTING` can rollback together or commit together. Child Rust test process is stopped by the parent's **REAL OS kill** before and after the same SQLite `BEGIN IMMEDIATE/COMMIT` boundary; cold readback verifies neither change survived pre-commit and BOTH changes survived post-commit. Identical original reservation IDs across X/Y remain isolated. A post-commit response-loss retry returns `AlreadyResolved`, not another send grant; original reserve/financial obligations remain exact. All original legacy v5 Rust tests still PASS.

**Still Draft / NO-GO:** This is a **private staged mutation**, not the production TradeService route or connected trusted original AccountStore source/census producer. The installed old AccountStore per-account worker and v5 RiskBook per-campaign worker have not been replaced with one production finance worker; offline source manifest remains `OFFLINE_NOT_AUTHORIZED`. Cloudflare old-owner server-side fence/epoch, complete authenticated source census, full original scoped account write paths, other RiskCommandActions, terminal/late/UNKNOWN, and physical installed Windows RB01â16/T09/T10 acceptance remain outstanding. **No merge, deploy, account use or Windows changes.**


## Previous #510 r4: original scoped RiskBook Summary and invariant â 72 direct tests (historical)

**Verified code SHA `05ac22a9a346536be244aed48e28fdb5c5040f0e`**, [GitHub CI #43621761540](https://github.com/iamaman11/okx/actions/runs/43621761540) **SUCCESS**: 65/65 filtered `okx-execution` RiskBook contracts, 6/6 kernel, 1/1 original AccountStore source, architecture guard/negative tests, rustfmt PASS. Filtered Rust tests ~1.08+0.00+0.11 seconds after build on Linux; **NOT** Windows/RB01â16/T09/T10 acceptance.

The ORIGINAL native `risk::book::store::integrity::verify_reservation_aggregates` now delegates to **one shared money-invariant implementation** that accepts either legacy v5 single-campaign SQL or fully scoped `installation_id`/`campaign_id` finance v6 SQL. Its risk entries, occupancy, strategy occupancy, loss sources, immutable no-reserve contradiction check, consumed amount and debt high-water queries all restrict the complete campaign identity; same historical reservation key can exist in two campaigns. The ORIGINAL RiskBook `BookMeta` decoder and `BookReadback` budget projection are also reused. A new private `read_unified_campaign_summary(&Transaction,...)` computes exact per-campaign read-only Summary without opening a connection or writer and refuses corrupted native reserves. The **existing finance importer calls this exact original invariant** inside the SAME account+campaign SQLite COMMIT, and cold readback calls it again. Two-campaign tests prove independent unchanged X summary, correct remaining budget and refusal of corrupted Y. All older native v5 tests still pass.

**Still NO-GO / Draft:** This is a **private scoped invariant and read-only Summary**, NOT a production `TradeService::Summary` route or grant; eight other RiskCommandAction operations have NOT been cut over, and none of the nine production callers currently owns unified SQL. The original AccountStore worker still opens account-local v2 SQL, and RiskBook owner still opens a separate campaign v5 SQL in production. Cloudflare server-side fence/owner epoch, authenticated full census, scoped original account write paths, all nine live actions, terminal/late/UNKNOWN, Windows RB01â16/T09/T10 26/26 are outstanding. The offline import marker remains `OFFLINE_NOT_AUTHORIZED`. **No merge/deploy/trade/Windows changes.**


## Previous #510 r4.0 offline manifest and OS-kill witness â 71 direct tests (historical)

**Verified code SHA `31b0438ee7a4c0febb39a66f544b61122603f12f`**, [GitHub CI #43620387365](https://github.com/iamaman11/okx/actions/runs/43620387365) **SUCCESS**: 64/64 filtered `okx-execution` + 6/6 `okx-kernel` + 1/1 original AccountStore fill/bill = **71 direct Rust cases**; architecture guard/negative self-tests and rustfmt PASS. Linux test execution after build ~0.90+0.00+0.07s, not full native Windows acceptance.

**One actual 39+8 offline import transaction now carries its own durable recovery witness.** The private importer requires `ExpectedFinanceInventory` containing the exact installation, account IDs and campaign IDs; missing/duplicate/foreign identities reject *before initializing target tables*. It copies the 47 original financial tables into the candidate finance SQLite under one `BEGIN IMMEDIATE/COMMIT`, calculates a deterministic scope+all-row SHA-256 across original values and writes a single `finance_import_manifest` receipt in that **same COMMIT**. Target schema now has **47 original finance tables + 1 migration metadata table (48 total)** and **48 original triggers + 2 immutable marker triggers (50 total)**. The extra table is *not* a second financial ledger.

`inspect_stopped_finance_import` verifies the exact expected roster, version, table census, original row totals, marker, full persisted content digest, SQLite integrity and FK in a read-only snapshot after reopen. A missing ACK is recovered by **readback**, not a second import; replay on nonempty target is rejected. Negative tests deny incomplete/duplicate manifest, altered financial facts despite valid SQLite integrity, and marker mutation. A real native child process physically imports all 47 original finance tables while parent **OS-kills before and after one SQLite COMMIT**. Before: no tables/debts survive; after: original account history, held debt, risk reserves and offline marker survive exact reopen/readback. Parent verifies actual child exit and boundary markers; no simulated failure is counted as a physical witness.

**Boundary:** This roster is supplied to the private importer and is **not an authenticated complete census from the production owner**. Its SHA-256 is a snapshot consistency proof, **not** Cloudflare server-side mutation revocation, a trusted source signature or an owner epoch. The stored marker's state is `OFFLINE_NOT_AUTHORIZED` and cannot grant a trading capability. No production AccountStore or RiskBook writer has been switched; all nine live RiskCommandAction callers, terminal/late/UNKNOWN, Cloudflare legacy server-side fencing/epoch, actual Windows RB01â16 and T09/T10 26/26 remain **NOT_PROVEN**. PR #511 stays **Draft / NO-GO**; no installed app, account or Cloudflare runtime changed.


## Previous #510 r4.0: 39+8 offline source rows before manifest and OS-kill (historical)

**Verified CODE HEAD `7519caa78c343d496d677d6dbcd24848b69eaed9`**, [GitHub CI #43615452874](https://github.com/iamaman11/okx/actions/runs/43615452874) **SUCCESS**: 60/60 direct execution-crate risk contracts, 6/6 kernel, and 1/1 original AccountStore fill/bill source (67 cases); architecture guard, negative self-tests and rustfmt PASS. Linux execution after build ~0.75 + 0.00 + 0.11 seconds. Not the full Windows/RB/T09/T10 acceptance.

`account_store::sql::unified_financial::importer::import_stopped_finance_into_empty` now copies all **39 source AccountStore and 8 source RiskBook v5 campaign tables** from an explicitly supplied set of stopped SQLite/WAL files into the ONE candidate v6 finance.sqlite under **one BEGIN IMMEDIATE/COMMIT**. All source OS writer leases are held in canonical path order; account/campaign identity, source SQLite version/table census/integrity/FK, original RiskBook **primary reserve vs cached aggregate** invariant checks, target scoped row parity/47 tables/FKs/triggers and original values are preserved. The existing RiskBook invariant validator is reused, not duplicated. Direct tests cover two original accounts, two old campaign SQL files carrying HELD debts and no-reserve fences, identical historical reservation keys across campaigns, full rollback on forged plan, active risk writer refusal and rejection of a corrupt source aggregate.

**Still Draft / NO-GO:** this is a private offline source-set witness, not a proof that the supplied source inventory enumerates every real account/campaign. Complete authenticated census/manifest/digest, *actual Cloudflare server-side revocation* with epoch/readback, crash-and-retry migration marker, production one-writer cutover, authenticated source writer, all 9 actions, terminal/late/UNKNOWN and physical Windows RB01â16/T09/T10 26/26 remain outstanding. `legacy_fenced=1` inside an old SQLite is not an authority revocation proof. No real account, demo, staging worker or Windows installation was altered.


### Previous #510 r4 account-only checkpoint â 63 direct cases PASS, Draft/NO-GO

**Verified code:** `25f28f7f7acac6ac9b2b6d2fde83268a46ab4a71` (importer + native source writer file-lock protection); [GitHub CI #43537742231](https://github.com/iamaman11/okx/actions/runs/43537742231) **SUCCESS** on documentation-only descendant `955a2935857f55736c874b30a775eab0ec879e3e`. `risk_book_contract` **56/56 `okx-execution` + 6/6 `okx-kernel`**; independent genuine native AccountStore fill/bill source proof **1/1** = **63 filtered cases**, including subprocess child entrypoints. `cargo fmt` and architecture guard/negative self-tests PASS. After compilation Linux execution ~0.71 + 0.00 + 0.13 seconds. **Not** full Windows 26/26, RB01â16 or T09/T10.

**Physical original-schema evidence:** `schema/finance-unified-v1.sql` creates exactly 47 **original** account+campaign tables (29 v2 account + 10 derived account + 8 Rust RiskBook) in **one physical SQLite/WAL**, with scoped original columns/PK/UNIQUE/FK, 21 original read indexes, 48 original immutable/invalidation triggers and v6 schema identity. The former account-local event cursor is now `(installation_id,account_key,event_seq)`; its insertion trigger is account-scoped. Direct tests check A/B duplicate original IDs, two account singletons, two campaign singletons, actual FK rejection, scoped native source invalidation, original bill immutability, index/trigger census, `PRAGMA integrity_check`/`foreign_key_check`.

**Real stopped AccountStore offline migration evidence:** `account_store::sql::unified_financial::importer::import_stopped_accounts_into_empty_finance` copies **all 39 original AccountStore tables, using their original columns and SQLite values,** from two real native AccountStore source DBs into one empty v6 target under a single `BEGIN IMMEDIATE/COMMIT`. It checks original schema v2, exact 39-table census, source identity/integrity/foreign keys, exact column parity, scoped row parity, target empty/WAL/FULL/FK, and final v6 integrity. It installs triggers **after** source-copy to avoid duplicating original receipt events. Local exclusive OS `.writer.lock` leases cover the source-copy window; a live original AccountStore owner is physically rejected **before any target table is created**. New test `risk_book_contract_unified_full_original_account_import_two_accounts` physically imports two closed original sources with identical original `intent_id`/decision IDs and survives target reopen; `risk_book_contract_unified_offline_import_rejects_active_native_writer` proves the native lock. Original source files are not modified. Existing real native partial receipt import remains a separate direct witness.

**Updated 14 Nov / do not mark GO:** all **47 original source TABLE ROWS** now have an offline scoped importer, including eight RiskBook campaign tables; private tests verify actual HELD and no-reserve obligations. Complete source inventory/authenticated export, distributed Cloudflare revocation/owner epoch, actual TradeService account+RiskBook one-writer cutover, all nine actions and real installed Windows/T09/T10 remain incomplete. Local file locks do **not** prove Cloudflare revocation. The earlier 10-table `finance-v1.sql` pilot continues privately only for historical tests; it is not a parallel production financial engine. [Exact code/schema/map](financial-sql-scope-r4.md). PR #511 remains Draft/NO-GO; no production install, account migration, REAL/Demo or legacy Cloudflare change.


### #510 r4 SQL identity and evidence guard â 59-case checkpoint

**Verified exact code SHA:** `9f62f2df354b58dccc9f2d2a6961a7fbad34bebc`, [CI #43533925640](https://github.com/iamaman11/okx/actions/runs/43533925640) **SUCCESS**. Linux direct filtered `risk_book_contract`: `okx-execution` **52/52**, `okx-kernel` **6/6**, plus original `AccountStore` SQL partial/terminal/late fixture **1/1** = **59** filtered test cases, executed in 0.58s + 0.00s + 0.12s after compile. This is **not** full Windows 26/26/T09/T10 or complete RB01âRB16.

New physical SQLite negative witnesses in the *unactivated scoped schema*: `risk_book_contract_unified_receipt_rejects_wrong_account_source_and_digest` prevents mixed A-reservation/B-source and wrong source digest at the FOREIGN KEY layer; `risk_book_contract_unified_rejects_missing_or_fake_native_receipt` denies absent original financial proof and rejects malformed/non-structural JSON at Rust source validation; `risk_book_contract_unified_one_original_intent_one_campaign_and_original_plan` prevents a single original account intent being charged in two campaigns or acquiring a forged plan hash. Existing real AccountStore original-source import, exact replay, A/B duplicate venue IDs and OS-kill before/after one local COMMIT continue to pass.

**Scope caveat:** mock receipts used in isolated A/B/failure fixtures validate shape and SQL invariants but are **not exchange-authenticated source**; trusted original AccountStore source is exercised separately during offline migration. The final 47-table normalized source producer, single production writer, multi-fill/late/terminal migration, Cloudflare fencing, Windows and full RB physical matrix remain **NOT_PROVEN**. [Detailed SQL table and source map](financial-sql-scope-r4.md). PR #511 stays Draft/NO-GO.


### #510 r4.0 source-authentic partial migration â verified original AccountStore producer

**Actual Rust code SHA `a1660565771a5e805f2c03d347840284b5a8d60f`**; [CI #43531489964](https://github.com/iamaman11/okx/actions/runs/43531489964) **SUCCESS**: 49/49 filtered execution Rust contracts + 6/6 kernel + 1/1 **real native AccountStore** `e1_partial_and_final_entry_settlement_are_monotone_and_not_double_counted` (extended to exercise legacy original-source import), formatting, architecture guard and negative self-tests PASS.

**New evidence:** within the real original AccountStore test, `inspect_legacy_partial_source` verifies the existing native `bind_partial_entry_settlement` and its original fill/bill/fee links. It imports original immutable `EntrySettlementEvidence` bytes and source identities into the scoped **one-file** finance schema. A single destination `Transaction` imports source + risk reservation and commits the original 50.01 USD as one campaign debit from the 100.05 upper bound, preserving remaining 50.04 and the original receipt. Exact reimport/replay and reopened SQLite do not double count. No second fee/FX logic. Checked original account order identity and stored source/bill digests on settlement.

**Limit:** this validates **one immutable partial source during isolated offline legacy migration**, not an authenticated production operation running on the fully normalized new ledger. The original AccountStore DB is still a **separate source file** in this migration test; the remaining 47-table schema/actual owner cutover, multi-fill/late/terminal imports, authority epoch and Cloudflare fencing are **NOT_PROVEN**. Historic two-file live authority remains unmodified; #510 r4 RB07/RB08/full source migration stay NOT_PROVEN. Detailed table map and accepted/blocked scope: [financial-sql-scope-r4.md](financial-sql-scope-r4.md).

### #510 r4.0 first scoped SQL commit â testable subset, NOT production accepted

[Source-defined 47-table scope inventory and schema/transaction index](financial-sql-scope-r4.md). Verified direct code SHA `941b24129697cd1399687c0736019f8a247721f7`, [Linux GitHub CI #43529137651](https://github.com/iamaman11/okx/actions/runs/43529137651) **SUCCESS**, original `risk_book_contract` filtered tests **49/49 okx-execution + 6/6 okx-kernel**, plus **1/1 original AccountStore fill/bill receipt** = **56** filtered cases (including subprocess entrypoints). Actual execution after build 0.44s + 0.00s + 0.11s. Architecture guard including negative self-tests and Rust formatting PASS.

The new `account_store::sql::unified_financial` accepts **one preexisting `rusqlite::Transaction` only** (no own Connection/writer/COMMIT), and uses `okx-kernel::risk::money::RiskAmount` for exact entry consumption. Its **10-table scoped subset** is on **one physical SQLite/WAL**. The fixture tests use two accounts whose identical original venue IDs coexist, shared campaign X and distinct campaign Y; invalid cross-account identity, repeated/stale proof, corrupt cache, forged risk-row-versus-immutable-receipt projection, nonzero reserved RELEASED, SQLite BUSY, pre-COMMIT rollback and real OS kill both before and after one COMMIT. The original SQL settlement source rows are manually provided by the isolated test fixture, **not** authenticated production venue/AccountStore producer inputs. No claim that the 39 existing account tables, eight existing campaign tables, full ledger, protocol/TradeService, policy/epoch, migration or Cloudflare fence have moved.

**RB07/RB08 status remains NOT_PROVEN for r4 production**, despite this first physical schema/transaction witness: owner wiring, full original SQL provenance under the same transaction, the complete normalized account schema, full historical migration and actual Windows performance are missing. Old two-DB historical tests below still execute but are superseded architecture evidence. No finance application runtime was activated.

## #510 r4.0 â single financial SQLite / single commit (NEW REQUIRED CONTRACT, NOT PROVEN)

**14 Nov 2026:** [Canonical Issue #510](https://github.com/iamaman11/okx/issues/510) changed its transaction and storage boundary to **one local file** `runtime_root/finance/finance.sqlite` and **one SQL writer** for all accounts and campaigns of one authoritative Rust Engine. `AccountStore` retains ownership of original fill/bill/FX evidence; `risk::book` retains policy, risk/grant calculations; they must operate under **the same `rusqlite::Transaction` and single COMMIT** when settling an account financial fact and changing campaign reserves/budget/receipt. Scoped PK/UNIQUE/FK/joins/cursors/immutable triggers must separate accounts (including identical original venue IDs) and campaigns. Unscoped account/campaign singleton tables from separate-file schemas require migration. No new framework or second financial owner. External OKX POST/network ACK remains outside SQL transaction and retains UNKNOWN recovery.

**CURRENT STATUS: NOT_PROVEN, PR #511 remains Draft/NO-GO.** Historical v5 campaign fixture, standalone `risk-book.sqlite` writer and two-database `AccountStore â RiskBook â AccountStore ACK` tests below represent the **superseded r3.1 design**. The historical [50/50 filtered cases](https://github.com/iamaman11/okx/actions/runs/43521514579) PASSED for that original two-file design; they are **NOT** RB07/RB08/production acceptance of r4. They remain an inventory of reusable pure rules, immutable original source witnesses, exact money regression cases and OS-kill harnesses. Do not add more two-file ACK machinery for r4. Do not delete original account history or invent empty campaign capacity.

**Replace RB07/RB08 physical witnesses:** on original single SQLite/WAL, verify one `BEGIN IMMEDIATE/COMMIT` updates *both* immutable account source/settlement and RiskBook consumed/reserved/receipt; inject genuine failure **before** COMMIT (neither changes), OS kill **after** COMMIT but before caller reply (both changes exactly once), stale/missing/source-conflicting retry (no extra financial effect). Test A/B same exchange IDs + independent campaign IDs, queue fairness, protected legacy import, negative real Cloudflare server-side authority fence and owner epoch. Existing RB01âRB16, T09/T10 and Windows P95 target remain required. See #510 r4.0 for exact acceptance criteria and scope. No merging, trading or production activation on these incomplete observations.

---

## #510 RiskBook transfer â checkpoint A inventory (IN PROGRESS)

**Candidate:** [Draft PR #511](https://github.com/iamaman11/okx/pull/511), base `8bd70ceab23472d286a40d147cc1cd8e3aa5bf9c`. The table below inventories **current TS/DO behaviour** from `risk_authority.ts`, `risk_book.ts`, `risk_bridge.rs` and `risk_authority.rs`. It is **not** Rust production-wiring evidence. No new RiskBook authority has been activated. The expected properties below are independent requirements that the Rust direct witnesses must establish against original SQLite state.

| RiskCommandAction | Baseline owner / entrypoint | Required persisted boundary and independent expected | Planned direct Rust witness / physical requirement | Status |
|---|---|---|---|---|
| `SUMMARY` | `handleNativeRiskCommand â readRiskBook` | Authorized generation, verified book and policy; no mutation; reserved/consumed/remaining from one committed snapshot; unknown opening never zero | `risk_book_contract_summary_read_only`; reopened DB + negative missing-book | NOT_PROVEN |
| `INSTALL_ENTRY_OPENING` | `handleNativeRiskCommand â installCampaignEntryProof` | Same-cohort opening source, campaign/installation ID, all accounts/history/provenance, fresh source and digest; atomic opening marker, exact retry idempotent; absent source not zero | `risk_book_contract_install_opening`; corrupt/incomplete/foreign/duplicate and restart | NOT_PROVEN |
| `ACCOUNT_SUMMARY` | `handleNativeRiskCommand â risk_account_v2 / risk_loss_v2` | Authenticated account scope, all aggregation rows coherent, no read-side mutation or fabricated zero for missing aggregate | `risk_book_contract_account_summary`; two distinct accounts and missing aggregate | NOT_PROVEN |
| `INSPECT` | `handleNativeRiskCommand â inspectRisk / inspectNoReserveFence` | Exact reservation key+plan, read-only state/sequence; distinguish persisted no-send tombstone from absent reservation | `risk_book_contract_inspect`; restart/tombstone/foreign identity | NOT_PROVEN |
| `RESERVE` | `handleNativeRiskCommand â reserveRisk` | Same SQL transaction checks current policy/session/cohort/source, applies monotonic loss, validates both entry and modelled-loss capacity, writes HELD + sequence + account/strategy/global aggregates; identical HELD replay not extra debit, same-key other payload conflict | `risk_book_contract_reserve_multi_account`; two contenders, new campaign isolation, old policy and reply lost | NOT_PROVEN |
| `SETTLE_ENTRY` | `handleNativeRiskCommand â settleEntrySpend` | Authenticated source/revision and cumulative entry USD; nondecreasing amount, exact replay read-only even after unrelated sequence changes, late fee after RELEASE adds spend but never subtracts another reservation, monotonic bound-breach | `risk_book_contract_settle_entry`; account SQL â campaign SQL ordering and reopen | NOT_PROVEN |
| `RELEASE_UNSENT` | `handleNativeRiskCommand â releaseUnsentRisk` | Verified native no-send/SQL proof; release HELD or persist no-reserve fence; no future RESERVE for same identity; no accidental release after POST/UNKNOWN | `risk_book_contract_release_unsent`; lost reply, late reserve and restart | NOT_PROVEN |
| `BEGIN_POST` | `handleNativeRiskCommand â beginRiskPost` | Fresh post-HELD causal reads, generation/sequence/policy/plan+source exact; persistent POSTED fence before a possible exchange POST; duplicate/stale/unknown cannot grant a second send | `risk_book_contract_begin_post`; OS kill before/after durable boundary, no double POST | NOT_PROVEN |
| `RESOLVE` | `handleNativeRiskCommand â transitionRisk` | UNKNOWN/OPEN/RELEASED transitions conditional on exact revision and proof; terminal release requires complete native census and ledger evidence; preserve obligations on missing/late evidence | `risk_book_contract_resolve`; partial fill, terminal proof, reply lost and reopen | NOT_PROVEN |
| init / lower-limit / governance | `initializeRiskBook`, `lowerRiskBookLimit`, `lowerCampaignEntryLimit`, `account_governance.ts` | New/legacy campaign distinguished; authoritative versioned policy, budget decrease never mints capacity; rollback/fencing denies legacy mutations after transfer | `risk_book_contract_policy_and_transfer`; negative real legacy fence in isolated Worker | NOT_PROVEN |

**Implemented in Draft PR #511 (partial, NOT acceptance):** `okx-kernel::risk` supplies exact money and capacity rules; private `okx-execution::risk::book` contains actual SQLite `BEGIN IMMEDIATE` reservation, WAL/FULL durability, persisted `HELD` receipt, canonical legacy reservation key, idempotent replay/conflict, distinct campaign file lease, and a bounded blocking SQL owner with Tokio-facing typed commands and read-only committed projection. Source-attested **cumulative account loss is now recorded monotonically in the same transaction**, including when an ordinary reservation is denied; a repeated source is not re-charged and a regressing source fails closed. The **test-only** bootstrap fixture does not authorize a new production campaign or prove legacy DO fencing. Source **authenticity** is NOT yet established by a production adapter; no trade caller, POST grant, settlement/release rules or safe authority transfer are wired. **Do not interpret this partial owner as complete financial authority.** On native Windows, the **isolated staging harness** ran `17/17 risk_book_contract_* PASS` (0.10 seconds of measured test execution, after compilation), including real child-process OS kill after SQLite COMMIT, WAL reopen, cross-process writer rejection, concurrent Tokio account requests, complete cohort/external occupancy, monotonic cumulative loss on denial, projection refresh, legacy 2â64-character strategy identity and JavaScript-safe integer bounds. Isolated `cargo clippy --tests -- -D warnings` and `cargo fmt --check` PASS. **At this earlier isolated-staging checkpoint**, actual repository Cargo/Clippy and the architecture guard were unverified; later filtered real-repository evidence is recorded below. Full Clippy and RB01âRB16 remain unverified. No full operation row above is marked PASS from this subset.

### 13.11.2026 â P0 RiskBook corrective slice (real repository Rust, NOT production cutover)

- [Draft PR #511](https://github.com/iamaman11/okx/pull/511), SHA `bd25f98f4bc42411fc5a7fa44fb6f1b5f788fcdc`; [CI #43505569002](https://github.com/iamaman11/okx/actions/runs/43505569002): **21/21 filtered Rust tests PASS** from the actual repository Cargo crates (`okx-execution` 17, `okx-kernel` 4); existing architecture guard/negative self-tests PASS; `cargo fmt --all -- --check` PASS. This was a Linux CI run; complete Windows workspace, full Clippy, RB01âRB16 and T09/T10 remain **NOT_PROVEN**. Two of the 17 execution tests are child entrypoints; the corresponding parent tests independently verify real OS kill/reopen and a second OS process being rejected.
- `risk_book_contract_sql_rejects_over_limit_other_account`: A has 11 USDT source occupancy against individual cap 10, B requests 1 with campaign cap 30. Entire policy cohort is checked before any new HELD; result is `AccountAlreadyOverLimit` and no reservation.
- `risk_book_contract_sql_corrupted_reservation_aggregates_reject_reopen`: corrupt materialized campaign total after durable A=6 while retaining its primary reservation. Reopen fails closed with `RESERVATION_AGGREGATES`, rather than granting B=6. Exact original obligations and remaining per-entry balances are stored in `risk_entries`, and checked against campaign, account and strategy materializations before readback and reservation.
- `risk_book_contract_sql_release_requires_zero_reservations`: SQLite CHECK prevents an apparent terminal `RELEASED` label with nonzero remaining obligations. The aggregate verifier lives under `store/integrity.rs` and uses an index on nonterminal rows; ordinary authorization does not scan the full closed history.
- The unactivated native risk-book fixture schema is now `PRAGMA user_version=2` after adding primary remaining balances; `risk_book_contract_sql_old_schema_version_is_rejected` proves that a previous version-1 file is refused rather than silently interpreted using the new columns.
- The two negative tests were committed **before** fixes (test-only SHA `fc2ccd0d24a3fb5ebaf99e38829d502a9cdad3ea`), but no standalone CI RED run was executed on that pre-fix SHA. The verified result is GREEN on the current SHA, not a claimed formal REDâGREEN certificate.
- `VerifiedCensus` still lacks a trusted production-native constructor. No policy bootstrap, non-HELD transitions, verified legacy Worker fence or production trade caller has been activated. No full RB01âRB16 row is marked PASS by this corrective slice.

### 13.11.2026 â Follow-on actual-Cargo SQL observation slice (INTERNAL ONLY)

**Code candidate:** [PR #511](https://github.com/iamaman11/okx/pull/511), SHA `8571b5d084e7e6ed51351c8c6f17d004ce43be46`. [Direct GitHub CI #43507009073](https://github.com/iamaman11/okx/actions/runs/43507009073): actual repository Linux Cargo `okx-execution` **20/20**, `okx-kernel` **4/4** (`risk_book_contract` prefix), zero failure; 20 includes two child test entrypoints whose parent tests actually control OS kill/reopen and process exclusion. Architecture guard and negative self-tests PASS; `cargo fmt --all -- --check` PASS. This is **not** full workspace Clippy/Windows or RB01âRB16 acceptance.

- `store/integrity.rs` now verifies `risk_meta.loss_consumed_usd` equals the exact sum of every registered `risk_loss_source.cumulative_loss_usd`. Missing per-account primary source or a lowered materialized loss total fails closed as `LOSS_SOURCE_AGGREGATES` before reserve/readback and after reopen. Direct negative test: `risk_book_contract_sql_loss_source_total_and_presence_are_durable`.
- `store/queries.rs` adds private typed read-only `account_readback` and `inspect` on the **same** serialized SQLite owner. Account readback verifies active reservations, exact USDT/USD obligation and cumulative loss, rejects foreign/absent identity and reopens identically. INSPECT requires the exact reservation key and immutable plan, observes the durable typed state and sequence, and preserves the last committed book sequence. Direct tests: `risk_book_contract_sql_account_observation_and_inspect_preserve_revision`, `risk_book_contract_sql_bounded_owner_answers_readonly_observations`.
- **Not a public production replacement yet:** existing `RiskAccountBudget` also requires authenticated scope and verified latest activity time, which this internal readback does not provide. A missing INSPECT row or wrong plan **is not** a durable `RELEASED`/no-send proof: the legacy `risk_no_reserve_v1` durable negative fence is not yet implemented in Rust. The private observation methods never authorize new exchange POST or claim settlement.

**Checkpoint A provenance boundary (verified existing source structure, not a completed adapter):** `okx-agent/src/trade_service/execute.rs::census_with_reads` is the current native producer, consuming the versioned governance cohort, verified private account/position/order snapshots, paginated derivative-algo evidence and `okx-execution::AccountStore` financial/policy reads, with identity/generation/freshness checks. It emits `okx-protocol::NativeRiskCensus` carrying canonical cohort keys, per-account occupancy/loss, content/source digests, source-read floor, generation-related evidence and completeness flags. `risk_bridge::NativeRiskClient` still transports that wire DTO to the legacy Worker. The new private `VerifiedCensus` fixture in `okx-execution::risk::book` **does not** prove that these facts came from the authenticated producer. Before production wiring, implement one typed provenance-preserving construction path from the real native source/authorized governance and confirm the same durable policy revision, epoch, cohort, causal frontier and post-HELD re-read. Valid-looking flags/digests alone must not become trust. No second census producer or transport-side authority.

### 13.11.2026 â Exact remaining-budget readback (INTERNAL, not authority cutover)

[Source SHA `c8729dfb586b209001c0aa4970021b118229394e`](https://github.com/iamaman11/okx/commit/c8729dfb586b209001c0aa4970021b118229394e); [direct GitHub CI #43507749101](https://github.com/iamaman11/okx/actions/runs/43507749101): **25/25 filtered Rust tests PASS** from actual source (`okx-execution` 20 including 2 internal child entrypoints whose OS witnesses are asserted by parent tests; `okx-kernel` 5). `cargo fmt` and existing architecture guard/self-tests PASS. GitHub Linux runner; still **not** full workspace Clippy/native Windows/complete RB01â16.

- `okx-kernel::risk::rules::remaining_budget` computes exact, nonnegative limit â consumed â reserved with checked arithmetic. If the committed debt exceeds a lowered limit (or intermediate sum exceeds the legacy 28-digit wire range), remaining capacity is exactly zero, never overflow-derived permission. Independent test: `risk_book_contract_exact_remaining_budget_never_mints_headroom`.
- `okx-execution::risk::book::store::BookReadback` now includes committed global modelled-loss limit/held, loss limit/consumed/reserved/remaining, and separate entry limit/consumed/reserved/bound-breach/remaining. Direct SQLite commit and cumulative-loss tests assert fixed exact amounts and reopen equivalence. Calculations happen in the pure kernel, not duplicated in the SQL owner.
- This is a private `SUMMARY` **subset**, not the existing public `RiskBudgetSnapshot`: authorized campaign inception, opening source/time, policy provenance and production ingress remain unproven, so no public summary authority is enabled. `ACCOUNT_SUMMARY` and `INSPECT` are also still private subsets; legacy negative no-reserve fence is absent.

### 14.11.2026 â True two-owner SQLite transaction-gap acceptance (PRIVATE, 50 tests)

**Actual source SHA:** `21ed8b30eaebae0ef67c7c711689cfdab2c04763`, [GitHub CI #43521514579](https://github.com/iamaman11/okx/actions/runs/43521514579) **SUCCESS**. `okx-execution` **43/43** + `okx-kernel` **6/6** filtered `risk_book_contract`, plus **1/1** original AccountStore source fill/bill test = **50 tested cases** (including parent-controlled child entrypoints). Architecture guard/negative self-tests and `cargo fmt` PASS. Direct test execution after build on Linux GitHub runner: 0.21s + 0.00s + 0.06s. Not a full RB01âRB16, 3-crate direct target, native Windows P95 or 26/26 T09/T10.

**Why there are exactly two independent SQL owners, not two financial engines:**
- `AccountStoreHandle` is the *original per-account evidence owner*: venue order/fill, linked bills/fees, immutable entry admission/FX and original SQL receipts. It is never allowed to allocate campaign-wide risk budget.
- `RiskBookHandle` is the *one campaign-wide budget owner* shared by all member accounts: modelled-loss reservations, global entry consumed/reserved, budget policy, causal BEGIN_POST and receipt sequence. It does not duplicate venue/fee calculation and has just one SQLite writer and one physical file for this campaign.
- Both run *inside Rust* with their own `rusqlite` connections and file leases. Distinct SQLite files do not share an atomic commit, even inside the same Tokio runtime; merging them would migrate the existing per-account ledger and all campaign members, not simplify this issue. No additional orchestrator/third writer is introduced. The one Rust call path uses native source read â campaign COMMIT â original account SQL ACK, with exact digest/revision replay for the crash gap.
- One campaign file per campaign ID is deliberate, independent of account files. A local file lock does not fence cross-host or legacy Cloudflare: durable owner epoch and authenticated legacy revocation are still **NOT_PROVEN**, so this is not permission for REAL trading.

**Now actually executed, not merely planned:**
- `risk_book_contract_real_source_two_sql_owners_commit_then_ack_once`: authentic AccountStore prepares the original intent/quote/admission and risk receipt, records native POST and an actual partial fill/fee bill/finance entry, and produces immutable original `EntrySettlementEvidence`. The same Rust `RiskBookHandle` consumes that source, commits exactly 50.01 USD while preserving 3 USD modelled-loss reservation, then writes the source ACK. Missing source is rejected; exact replay after ACK is read-only.
- `risk_book_contract_real_source_two_sql_owners_recover_missing_ack`: the campaign commits the source-backed 50.01 before account ACK; after closing/reopening the campaign SQL owner, original account SQL is demonstrably `partial_entry_ack_pending=true`. An original-stale-sequence exact replay writes the pending ACK without increasing consumed money.
- `risk_book_contract_sql_real_two_owner_os_kill_after_campaign_before_account_ack`: a **real child OS process opens both source SQL and campaign SQLite/WAL**, independently verifies the immutable original fill/bill receipt, COMMITs the original campaign entry debit and publishes its post-COMMIT witness; Rust parent kills the child *before original account ACK*. Parent reopens BOTH original SQLite files under their real owners, finds committed campaign 50.01 and pending account ACK, then replays precisely that revision. Source ACK becomes durable; budget book sequence, charged and reserved USD, and modelled-loss reservation do not change. No fake response-loss flag substitutes for the physical kill.
- The older parent-controlled `risk_book_contract_sql_os_kill_after_partial_entry_debit_is_exact` remains as the narrower single-campaign-file witness. The source-forming test is an additional independent native input assertion, not a second accounting model.

**Still open:** full terminal `SETTLE_ENTRY`, release/late fees, UNKNOWN/OPEN/RESOLVE, trusted producer/policy/epoch, legacy Worker fence+complete transfer, native production caller graph, negative cross-host proof, real Windows/whole-target performance. **No demo/REAL, installation, or deployment changed; PR stays Draft/NO-GO.**

### 14.11.2026 â Native partial SETTLE_ENTRY and source SQL ACK (PRIVATE, 46 tested cases)

**Code SHA:** `a23b1ba59deb30643423e3a41533be3f05e00891`; [CI #43518944934](https://github.com/iamaman11/okx/actions/runs/43518944934) **SUCCESS**; actual `risk_book_contract` Rust tests **39/39 `okx-execution` and 6/6 `okx-kernel`**, plus **1/1 original AccountStore fill/bill partial-and-terminal receipt test**. Architecture guard and negative self-tests, rustfmt PASS. After compilation: 0.31s + 0.00s + 0.10s on Linux GitHub CI. Filtered test counts include OS-child entrypoints whose actual effect is asserted by their parent tests; this is not the full RB01âRB16/Windows 26/26/whole workspace acceptance.

**No duplicate money logic:**
- The original `AccountStore` SQL owner is still the only producer of `EntrySettlementEvidence`, based on captured fills, linked fees, original entry admission and frozen USD/USDT conversion. `AccountStore::partial_entry_settlement_receipt` returns the existing immutable original revision linked to its latest source head, intent, central reservation, original plan and venue order, refusing missing/mismatched evidence. Source test `e1_partial_and_final_entry_settlement_are_monotone_and_not_double_counted` exercises this readback with actual captured fill/bill facts.
- The same bounded campaign `RiskBookHandle::settle_partial_from_account_sql` reads that native receipt, executes `RiskBookStore::settle_partial_entry` in one `BEGIN IMMEDIATE` transaction (exact generation/book CAS, only POSTED, exact plan+account+reservation, monotonic cumulative USD/revision/source digest), and **only after a successful campaign commit** writes `AccountStore::acknowledge_partial_entry_settlement` in the original account SQL owner. These are deliberately two separate durable commits, not an invented global atomic transaction. If the account ACK fails after campaign commit, return `OutcomeUnknown` and recover using exact idempotent revision replay; never debit again.
- Unused `reserved_entry_usd` is reduced by the actually consumed amount, `entry_consumed_usd` is incremented once, but the original modelled loss and loss-USD reserves are **not** released. Above-bound actual spend is recorded in full and the monotonic `entry_bound_breach_usd` gate denies new entries. The opening-entry baseline is separately persisted to distinguish historical consumption from new receipts.
- SQLite fixture schema **v5** has one immutable cumulative entry amount/revision/source digest per reservation, primary remaining entry obligations and an index-free initial implementation of original SQL receipt reconciliation. The verifier compares primary remaining obligations and summed source-settled amounts + opening to cached campaign consumed/reserved totals; corrupted projections reject reopen instead of creating headroom. No production DB initialization or migration was activated.

**Executed direct witnesses:** `risk_book_contract_sql_partial_entry_consumes_reserved_budget_once` (two committed partial revisions, exact lost ACK, conflict/reopen, never double debit), `risk_book_contract_sql_partial_entry_regression_and_breach_fail_closed` (over-upper actual fee latch and monotonic revision), `risk_book_contract_sql_partial_entry_integrity_detects_forged_cache` (corrupt cached entry consumed fails on reopen), and `risk_book_contract_sql_os_kill_after_partial_entry_debit_is_exact` (an independently killed OS child committed the debit, lost its reply, parent reopened the ORIGINAL WAL/SQLite and proved no duplicate debit). Source fixture `e1_partial_and_final_entry_settlement_are_monotone_and_not_double_counted` is executed in the **same CI job**, using the original native SQL code.

**Historical 46-case checkpoint, superseded by 50-case two-owner physical verification above:** at that earlier point an OS-killed two-owner ACK interleaving was pending. It is now executed (see exact SHA/CI above). Terminal `SETTLE_ENTRY`, late fees after release, `UNKNOWN/OPEN/RESOLVE`, authenticated policy+owner epoch, real production source constructor, Cloudflare fence/import and all nine production callers are still unproven. A late fee arriving between campaign COMMIT and native ACK still needs its own causal test.

### 14.11.2026 â Native BEGIN_POST durable CAS (PRIVATE / 40 direct tests)

**Actual code SHA:** `3f2123d0da33e740ea6457e9a37a49f44ea237e5`; [CI #43516077150](https://github.com/iamaman11/okx/actions/runs/43516077150) **SUCCESS** â actual source `cargo test --locked -p okx-kernel -p okx-execution --lib risk_book_contract -- --nocapture`: **34/34 `okx-execution` + 6/6 `okx-kernel` = 40/40 filtered tests**. Architecture guard + negative self-tests and cargo fmt PASS; after compilation, 34 execution tests 0.25s and kernel 0.00s on GitHub Linux. Some filtered child entrypoints are counted and intentionally no-op outside their parent-controlled process; the parent tests **do execute** the child and assert its physical boundary. This is not a full 3-crate target, Windows P95, T09/T10, or RB01âRB16 acceptance.

**Rust code, no public trading adapter:**
- `okx-kernel::risk::model::{PostCausalProof,PostAccountRead}` and pure `validate_post_causality`: exact reservation, original Native incarnation, canonical policy cohort, per-account independently identified private source reads, monotonic after-HELD completion and â¤5,000ms proof window. Duplicate source read IDs are invalid. These types/hex hashes by themselves **do not authenticate who gathered the evidence**.
- `risk_entries` fixture schema **v4** preserves `held_at_ms`, `reserve_read_floor_ms`, `native_incarnation` and one-way `post_sequence/post_authorized_at_ms/post_proof_digest/post_source_digest/post_existing_occupied_usdt`. `RiskBookStore::begin_post` executes one immediate SQLite transaction: original HELD/plan/campaign policy/generation/expected book CAS; fresh and complete full cohort; current external + all central reserves; **every** account cap and global cap; persisted account entry-upper limit; loss/entry budget/bound breach; exactly one POSTED transition and original amounts still HELD as obligations.
- A native cumulative-loss high-water update that makes POST unaffordable is committed in its own transaction scope, but POST stays HELD. Unknown/lost response to a committed POSTED transition never gives another authorization; reopening must read the original POSTED state, its proof and outstanding reserves. `verify_reservation_aggregates` now verifies POSTED and HELD primary financial obligations and recorded post-fence fields; unimplemented UNKNOWN/OPEN semantics still deliberately fail closed.
- Independent actual SQLite tests `risk_book_contract_sql_post_fence_commits_once`, `risk_book_contract_sql_post_fence_rejects_stale_causal_and_other_account_exposure`, `risk_book_contract_sql_post_loss_high_water_is_durable_on_denial`, `risk_book_contract_sql_posted_requires_durable_proof_fields`, and `risk_book_contract_sql_account_entry_limit_rejects_before_held`; Tokio owner test `risk_book_contract_sql_tokio_owner_posts_one_original_grant`; physical OS child `risk_book_contract_sql_os_kill_after_post_fence_never_regrants` kills writer *after SQLite POSTED commit before reply*, reopens the original DB/WAL, verifies one reservation and denial of another POST grant. Pure kernel `risk_book_contract_causal_post_requires_exact_cohort_and_incarnation` includes missing/duplicate/source-order/old-incarnation negatives.

**Not yet accepted as production BEGIN_POST:** the `PostFenceInput`/`VerifiedCensus` producer is still private; the existing `TradeService::census_after_held` sends the original `NativeRiskCensus + CausalPostProof` to legacy Worker, not this Rust owner. The typed private input's booleans, read IDs, wall time and digest are not a substitute for source-generation/identity verification at the original native readers, authenticated governance/owner epoch, current account SQL POST claim and full source-proof digest binding across the wireâdomain conversion. There is no native venue send from this module. The legacy Cloudflare authority remains active; **NO-GO** until source trust, owner transfer and negative Worker proof, all nine RiskCommand actions, settlement, UNKNOWN/terminal recovery and complete final Windows/RB acceptance.

### No-reserve / real AccountStore direct acceptance (31/31)

**Actual code SHA:** `1ee43ca4c4f2f1d0a922f07f3c45a41c7959e5c0`; [GitHub Actions #43511719826](https://github.com/iamaman11/okx/actions/runs/43511719826) **SUCCESS**, `okx-execution` 26/26 and `okx-kernel` 5/5 filtered `risk_book_contract` tests; architecture guard and negative self-tests PASS; `cargo fmt` PASS. Runtime after compilation: 26 execution tests 0.21s, five kernel tests 0.00s on Linux CI. This is **not** Windows 26/26 T09/T10, full workspace, proof of the Cloudflare legacy fence, or complete RB01âRB16.

**Added direct witness coverage:**
- `risk_book_contract_sql_durable_no_reserve_tombstone_prevents_future_grant`: no-row before proof stays unknown; durable negative fence, exact replay, altered record/plan rejection, reopen, subsequent grant on different account without capacity loss.
- `risk_book_contract_sql_unsent_held_release_is_exact_and_durable`: original `HELD` reserve released with committed SQL receipt; all three cached reserved capacities, account/strategy obligations and primary remaining balances return to zero once. Bad generation, stale CAS, altered plan/reason, lost ACK and reopen cannot release twice.
- `risk_book_contract_sql_rejects_invalid_no_send_and_conflicting_fence`: invalid proof cannot mutate; out-of-band positive reservation + negative fence triggers durable corruption error on reopen.
- `risk_book_contract_sql_owner_serializes_negative_fence`: native Tokio/SQLite owner's actual queued mutations and readback.
- `risk_book_contract_real_account_sql_unsent_proof_bridges_campaign_owner`: real `AccountStoreHandle` prepares/marks a source SQL reserve attempt; campaign SQL creates HELD; missing account SQL terminal proof refuses release; only a committed `REJECTED_UNSENT` with no native POST claim releases the original budget. Same source receipt reconstructs a replay without double refund.
- `risk_book_contract_campaign_db_path_is_installation_scoped`: deterministic cross-platform campaign identity location, invalid/path-traversal input rejection, and *no implicit budget creation*.

**Historical 31/31 checkpoint (superseded by the later v5 entry-settlement section above):** this bridge is still private inside `okx-execution::risk::book`; `TradeService` still sends commands to legacy Cloudflare Worker. The test constructs isolated SQL fixtures with no exchange or live client. Neither a fake native `VerifiedCensus`, a typed `UnsentSqlReceipt`, nor `legacy_fenced=1` permits REAL cutover without authenticated policy/epoch and real Worker fencing. Other terminal/POST/entry transitions remain missing.

### Checkpoint A native file identity and SQL no-send handshake (14.11.2026, partial)

**Chosen runtime location, not yet a production bootstrap:** the single campaign DB is `runtime_root/risk/campaigns/<lowercase-64-hex-campaign-digest>/risk-book.sqlite` where `runtime_root` is the active installation's native root (`C:\\okx\\runtime` on the installed Windows machine). `campaign_db_path` validates the canonical `sha256:<hex>` campaign identity and absolute runtime root without creating a file or risk budget. A different campaign receives a different file path; every account in the same campaign shares **one** DB. The existing `RiskBookStore::open_existing` canonicalizes that existing file, checks `installation_id`, `campaign_id` and a local exclusive `.writer.lock` lease. This OS file lease protects only this host, **never** multiple hosts or legacy Cloudflare.

**Current persisted fixture schema:** version **5**; `risk_entries` has exact primary remaining risk/entry obligations, immutable unsent-release proof digest/request hash/reason, and original HELD plus committed POSTED causal fields. A new `risk_no_reserve` table persists a negative fence keyed by the full original reservation address; `INSPECT` reports `RELEASED` from a real tombstone but **absence alone reports unknown**. The same SQL owner rejects contradictory negative/positive rows, stale CAS/generation, altered replay proof and a later `RESERVE` on the fenced identity. No automatic v2/v3/v4âv5 migration: no production campaign DB is initialized or activated. Only test fixture creation is exposed.

**Proven source-side link (negative path only):** `AccountStore::unsent_release_receipt` is an existing SQL query joining the native intent, matching reserve-attempt/receipt, terminal `REJECTED_UNSENT`, original request/plan and empty native POST/exchange-order markers. Its join additionally rejects a foreign account or plan in an existing risk receipt. `RiskBookHandle::release_unsent_from_account_sql` obtains this receipt by calling the real `AccountStoreHandle` owner and hashes its serialized durable contents before the campaign transaction. A missing/failed Account SQL proof blocks release. The two SQLite commits are **not** misrepresented as one transaction; replay is identity-bound and recoverable. Direct source-to-campaign test: `risk_book_contract_real_account_sql_unsent_proof_bridges_campaign_owner`. The final durable account release ACK/reconciliation wiring is still pending.

**Checkpoint A blocking decisions not yet implemented:** an independently authorized `CentralGovernanceSnapshot` policy installation into the campaign DB, monotonic owner epoch beyond a transient WS generation, authenticated legacy DO server-side revocation, complete validated transfer export/import with exact sums/receipts/negative fences, campaign opener's finance provenance, and post-HELD causal-source construction from the existing native census producer. These are required before creating/wiring a production risk budget; a test `legacy_fenced=1`, client `complete=true`, digest alone or local file lock cannot satisfy them. The complete 9-action production caller graph and remaining account SQLâcampaign SQL ACK/POST interleavings are still NOT_PROVEN.

**Native runtime observation (read-only, 13.11.2026):** installed Windows root `C:\\okx\\runtime` contains `engine.lock`, `account-catalog.pb`, `accounts/` and native `okx-agent.exe`. In `okx-agent/src/main.rs`, non-initialization commands acquire an exclusive `config.root/engine.lock` lease; each existing account storage is `account.sqlite` under its own root. `TradeService::update_governance` currently receives a validated `CentralGovernanceSnapshot`, sequentially applies the revision to connected **account SQL owners**, then updates its in-process snapshot. **These facts do not yet prove an atomic shared campaign policy revision**: the new RiskBook must validate its own authoritative durable policy/revision and fail closed on partial propagation. Do not confuse existing process lease with confirmed legacy DO fencing or multi-host fencing. The per-campaign DB path is now selected and tested as a pure derivation; production bootstrap/lease epoch, governance transfer and legacy-fencing remain explicitly unproven.

**Checkpoint A still NOT CLOSED:** the campaign SQLite path, test schema v5, private BEGIN_POST and partial SETTLE_ENTRY transitions and negative Account SQL â campaign SQL release handshake above are pinned, but production schema activation/upgrade, policy source+revision+owner epoch, legacy snapshot export/import, authenticated post-HELD producer and terminal identities, the complete caller graph (including recovery, summary, opening and fees), and the remaining terminal/late-fee two-owner ACK interleavings need code and real witnesses (the partial-entry original-source, positive ACK, and OS-killed ACK gap are now proven above). Confirm every exported `risk_book.ts` function and caller; this table is an initial finite operation index, not proof of complete reachable-call graph.

**Performance contract:** direct domain tests may execute in milliseconds; #510 still requires actual SQLite, required OS-kill/reopen and legacy fencing physical witnesses inside the measured target, plus static checks/review separately. T09/T10 coverage may only be removed when the old authority is factually gone and the same invariant has a new proven owner.



**Canonical:** [product #448](https://github.com/iamaman11/okx/issues/448), [execution #449](https://github.com/iamaman11/okx/issues/449), [bounded simplification #501](https://github.com/iamaman11/okx/issues/501). This is an evidence map, **not** a new suite, an authorization to trade, or a PASS certificate.

**Current candidate:** `fix/windows-clippy-required-495-20261107`, Draft PR #499.
**Accepted regression baseline: `8b5b0b0ba053bf2a80d57412c0c01811e1191c86`,
tree `bc7b75dd944b5c13523f1c5a5fe2aa930045af38`.**
[Required CI 42663259376](https://github.com/iamaman11/okx/actions/runs/42663259376)
completed SUCCESS, including release build. The
[matrix artifact](https://github.com/iamaman11/okx/actions/runs/42663259376/artifacts/13697122711)
contains all 26 distinct required/selected/executed IDs, 26 PASS, zero failures or
missing cases, 221 matching expected/actual assertions, 26 readbacks, and 90
connected F0âF8 crash trials reopening the original SQL roots. Some cases share
a cohort transcript; these are not 26 independent full campaigns.
The matrix completed in 1,169,864 ms; the same-SHA local run passed in 1,213,824 ms.
T09.6 also retains all nine actual POST attempts and independently verifies
the eighth/ninth cancellation targets and account executor scope.
The [Windows bundle](https://github.com/iamaman11/okx/actions/runs/42663259376/artifacts/13695894849)
was downloaded and its source SHA/tree, Cargo.lock and all three executable hashes
were independently verified. The Windows Cargo.lock bytes use CRLF; their hash
matches CI and the normalized content matches the source checkout. New changes require their own checks; this baseline
does not certify an untested descendant, installed workflow or REAL trading.
The earlier `32ee438e` and `0680e90f` baselines remain historical evidence below.

### Measured CI cost at the accepted baseline

Source: Windows job 129320134917 in run 42663259376 (attempt1). These are
observed step intervals, including the work performed by that step; they are
not isolated compiler or kernel benchmarks. Windows job wall time was29m47s.

| Step | Observed duration | Meaning for the development loop |
|---|---|---|
| Toolchain / Rust cache restore | 14s / 32s | Setup for this runner/cache state |
| First entry-settlement direct suite | 91s | Includes first build; not direct-test execution time alone |
| Cleanup / unsent direct suites | 4s / 3s | Warm subsequent checks |
| Native Clippy | 32s | Required static checks |
| Scenario lifecycle | 5s | Existing eight Windows boundary tests |
| Native crate tests | 177s | Includes test-target build and execution |
| Locked workerd dependency install | 8s | Existing production test assembly |
| Full26 | 19m30s | Dominant acceptance cost, including90 independent OS-kill trials |
| K9 two-heavy boundary | 2s | One scoped boundary; not full public K9 acceptance |
| Release binaries | 226s | Deployable build after acceptance |
| Evidence / bundle upload | 2s / 4s | Publication after each completed result |

The direct defect loop uses the appropriate owner filter after one compatible
build. Repeated full26 is not used for hypothesis discovery. These intervals
do not prove removable duplicate work or justify weakening any release gate.
Artifact names must use the checked-out PR head (falling back to github.sha on
non-PR events); a merge-ref suffix is not the source identity of these binaries.

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
| `ConnectAccount` | `query::execute` â `AccountRuntimeRegistry::connect` â `AccountBootstrapper` | `accounts::tests::prepared_account_rejects_unverified_identity_before_sql`; `recorded_two_account_bootstrap_opens_independent_sql_owners` (bootstrap/SQL, partial) | No focused test drives the public query arm through credential resolution and publication. |
| `Market` | `query::execute_market` â `AnalyticsService::market` â `MarketBootstrap` / existing research job owner | `analytics::market::tests::prepared_screen_8x100_latency_and_response_budget`; `market_source_scope_is_bound_to_native_public_client_environment` (partial) | No focused public-dispatch test covers the whole request-to-response route. |
| `MarketCapabilities` | `query::execute` â `AnalyticsService::market_capabilities` | `analytics::tests::market_capabilities_discloses_supported_operator_names_and_units` | Public dispatch is covered by query::public_dispatch_tests::capabilities_and_mailbox_use_public_dispatch_without_external_effects. |
| `MarketMicrostructure` | `query::execute_market_microstructure` obtains admitted fresh WS snapshot; `microstructure_summary` is pure projection | `query::microstructure_test::microstructure_projects_exact_spread_and_sweep_without_io`; `microstructure_rejects_crossed_book_before_sizing`; `bounded_book_sweep_rejects_insufficient_depth` | Query helper tests do not prove live WS transport freshness; that remains owned by `PublicWsHandle` tests. |
| `Research` | `query::execute` â `AnalyticsService::research` â job catalog, source adapters, `CpuExecutor` | `analytics::tests::immutable_result_pages_and_restart_catalog`; `c17_verified_export_reads_all_page_checksums_and_exact_rows` | These prove job/artifact contracts, not an exchange research result being profitable or a trading signal being approved. |
| `Trade` | `query::execute` â `TradeService::trade_with_invocation`; the existing public `trade` entry forwards to the same implementation; action dispatch in `trade_service/mod.rs`, `execute.rs`, `reconcile.rs`, `protection_cleanup.rs`, `entry_settlement.rs` | Direct suites: `entry_settlement_direct_tests`, `protection_cleanup_direct_tests`, `unsent_release_tests`; integration cases T09/T10 | Direct terminal owner stale-fill rejection now has T1 proof (see revision9 table); positive full cohort census and lost-reply orchestration remain separate. No fake census substitutes for that proof. |
| `Strategies` | `query::execute` â `TradeService::strategies`; worker loop `strategies::run` â `maintenance_turn` / `strategy_turn` | `strategies::tests::strategy_risk_budget_not_notional`; integration strategy cases under T09/T10 (see case records) | No complete direct per-action acceptance for Create/Update/Start/Stop and no proof that a generated strategy is profitable. |
| `Portfolio` | `query::execute` â `portfolio::portfolio` â selected account SQL owners | `portfolio::tests::summary_requires_complete_requested_window_and_fresh_account_nav`; `four_accounts_preserve_strategy_and_unattributed_finance_isolation` | Does not prove historical USD expense import for every managed account; incomplete source coverage remains partial. |
| `PortfolioScenario` | `query::execute` â pure `portfolio::scenario` calculation | `portfolio::tests::virtual_risk_has_no_exchange_side_effects_and_requires_psd` | Scenario inputs are user supplied; this is not a market-data backtest or exchange execution. |
| `Diagnostics` | `query::execute` â `diagnostics::diagnostics`; operation view reads the local projection; finance view reads the existing cycle trace; default account details still queue the SQL owner | `diagnostics::tests::default_diagnostics_never_claims_a_remote_risk_summary`; `operation_projection_reads_are_local_bounded_and_isolated`; `distinguish_sql_queue_deadlines_from_real_storage_outages` | Cleanup startup reconstruction restores only retained durable facts; ACK/initiator/history remain unknown where unrecorded. Public blocked-owner behavior and other families are not proved by the local getter benchmark. |
| `FindInstruments` | `query::execute` obtains `PublicWsHandle::reference_snapshot`; `find_instruments_response` maps the protocol enum and delegates filtering to `ReferenceRegistry::find_instruments` before the 50-row response limit | `query::instrument_search_tests::find_instruments_filters_type_currency_and_asset_before_public_limit` directly exercises the production helper with mixed SWAP/FUTURES, currencies, assets and more than 50 leading exclusions | Public dispatch also exercises instrument_search_routes_type_filter_before_public_limit; actual live reference refresh remains owned by PublicWsHandle. |
| `MarketOverview` | `query::execute` â demand WS instrument â fresh WS snapshot; on warming only, existing REST market snapshot plus explicit degraded quality | `query::microstructure_test::healthy_ws_overview_omits_unbounded_depth_and_fits_mcp_envelope`; `accounts::tests::recorded_public_ws_frames_make_production_market_snapshot_fresh` (partial) | No focused test covers the overview's full fresh-WS and REST-degraded branches together. |
| `AccountSummary` | `query::execute` resolves one account and reads its latest snapshot with `ACCOUNT_MAX_AGE_MS` freshness check | `accounts::tests::recorded_ws_login_business_ack_and_six_history_pages_capture_both_sql_accounts` (source path, partial) | No focused test covers this query arm's missing/stale snapshot responses. |
| `MailboxTelemetry` | `query::execute` returns installation identity from `AccountRuntimeRegistry` | query::public_dispatch_tests::capabilities_and_mailbox_use_public_dispatch_without_external_effects | Read-only response has no independent delivery/telemetry persistence proof. |

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
| Trade maintenance/recovery | `TradeService::strategies::run` â `maintenance_turn` â `reconcile_restored` / bounded per-intent `status_intent` | T09.7 and T10-11 recovery case records; `accounts::tests::recovery_prepared_account_reopens_catalog_and_original_sql` | These are scoped recovery paths; they do not establish an installed application's real account restart/recovery acceptance. |
| NativeâWorker session | `RuntimeSession` is the DO/session owner; `webSocketMessage` authenticates generation and dispatches risk control; `webSocketClose/Error` reject pending work | `runtime_session.test.ts`; `risk_authority.test.ts`; `cloudflare_transport.rs::stale_native_risk_reply_cannot_grant_a_different_generation` | Worker unit/composed fixtures are not physical WSS/network proof. |
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
| Research grid report | Train-only selection; per-candidate OOS/feesÃ2 summaries; immutable pre-compute input manifest, verified export and reference-aware retention | `okx-research` strategy registry tests, analytics grid/provenance/export/retention tests; legacy artifacts without the new fields remain supported |
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

### Completed local boundaries in the revision9 candidate

These are scoped local proofs, not a replacement for the final exact-SHA
full26/required CI/bundle or installed acceptance. The regression baseline in
the header remains unchanged until those artifacts are accepted.

| Packet | Production owner and real check | Accepted locally / remaining boundary |
|---|---|---|
| T1 | `terminal_release::release_terminal`; `trade_service::terminal_release::tests::stale_fill_coverage_blocks_release_before_worker` | Real SQL grant and canceled zero-fill order, venueuTime100/fills99, two invocations: exact typed identity/reason, unchanged SQL/money/receipt and zero risk/venue calls. Positive census, filled-close, lost-reply and reopen orchestration remain separate. |
| D1 | `query::dispatch` â `diagnostics::operation_diagnostics`; `diagnostics::tests::public_operation_diagnostic_survives_busy_owners_and_unknown_history` | Known cleanup projection survives busy real SQL/source owners. Route mismatch/unknown history and all HTTP attempts are checked; post-release FIFO fence excludes hidden SQL enqueues. Seeded projectionâpublic consumer proof, not real ACK production or process restart. Terminal release is not published in this cleanup trace. |
| R6 | `AnalyticsService::research` â `analytics::decision`; `analytics::decision::tests::` | Ten tests cover typed decisions, immutable revisions, actual catalog migration/reopen, conflicting/identical concurrency, public query dispatch, corruption/expiry/scope, retention and reply bounds. Existing Jobs/artifacts remain owners. Single-experiment results only; orphan prepublication files and large-result latency remain explicit limits. |
| C20 | `StrategyKernel::from_id`, existing replay and `Sources::candles`; `c20_` filters in kernel/research/agent plus protocol `composition_contract_tests::` | RollingMean reused; selectable EMA+momentum composition has literal signals/next-open trades/PnL and no-lookahead oracles. Actual decoder/source/cacheâmarket/research tests bind HTTP query, reject duplicates, block an in-window unconfirmed gap and preserve13.5/17 positive oracles. No finance/TradeService/routing changes. |
| L1 | Existing actual Native/workerd driver and `native-workerd-lifecycle.test.mjs` | Four Windows outcomes PASS141.126s, eight distinct Native processes; saved JSONLs/source hashes independently checked. Success has real WSS/public/source joins and two SQL termination watches. Error/timeout use explicitly labelled forced OS closure; post-dispose cleanup error retains primary+secondary failures and remaining cleanup. This does not prove graceful Rust task joins on forced exits, hard outer kill or every later phase; whole S1-05 remains open. |

Measured navigation without author help: Diagnostics56s, Research decision40s,
composition34s, and Trade terminal coverage43s (builds excluded). These are
controlled navigation exercises, not historical defect-localization timings.
Normal command forms: `cargo test --locked -p okx-agent --lib <filter> -- --nocapture`;
use the full module path with `--exact`. For L1, precompile the exact Agent lib
then `node --test scripts/trade-integration/native-workerd-lifecycle.test.mjs`.
Set `OKX_LIFECYCLE_ARTIFACTS` to retain both successful and failed stage evidence.

### Combined revision10 packets from checkpoint95

Source baseline `95a9b58aea14dcb57035c1d8f7be91f3b43e1921` is archived by
`checkpoint/501-local-95a9b58a`. Its independently verified local full26 took
1,224,259ms and contains221 matching assertions,26readbacks and90 original SQL
crash roots. That tag is immutable source backup, not GitHub-CI or installed
acceptance. Current descendant checks and exact source are recorded in the
existing #501 checkpoint; old full26 does not certify the descendant.

| Packet | Implemented boundary and focused command filter | Proven scope / limits |
|---|---|---|
| T2 / E2 | `terminal_release::tests::stale_bill_coverage_blocks_settlement_after_inspect` | Same production release twice with actual SQL reservation, fills100/bills99/uTime100:2 scoped scripted INSPECT,0SETTLE/RELEASE/venue effects, exact BillCoverage identity/reason and unchanged SQL/receipt/cursors. Scripted risk transport is not real Worker/cohort positive proof. |
| D2 / E1 | `terminal_release_trace`; both stale coverage tests in `terminal_release::tests::` | Actual release owner publishes reached stages. The public operation view reads `terminal_release:<intent>` while actual SQL/source owners are held; known stage/first blocker/identity survive,0 explain-caused SQL/HTTP/risk. FIFO fence checks hidden SQL jobs. Bounded128-intent projection exposes drops/evictions/superseded attempts, unknown history/durable state/initiator. Not durable restart reconstruction or a complete causal history. |
| R7 / E3 | `analytics::candidate_progress::tests::` | Six tests: actual submitted2Ã64 grid returns128 candidates/386 replay units; public Status barriers and observed/plain result equality; cancellation/late/Interrupted/reopen; additive legacy migration; unfinished candidate; bounded phase/count/monotonic/fixed-total rejection. CPU callback has only atomic observations; existing job task/catalog publish coalesced progress. Source cache is seeded in submitted-job proof; separate C20 raw-source proof remains distinct. |
| Q1 / E3 | `query::public_dispatch_tests::portfolio_scenario_` | Public dispatcher uses real virtual portfolio owner with literal2-position variance25/volatility5 and exposure/stress/margin oracles. Three-position non-PSD and invalid parameters reject. Artifact paths/bytes and HTTP/risk attempts unchanged. Caller assumptions do not become market observations or financial account facts. |

Commands use the existing form `cargo test --locked -p okx-agent --lib FILTER`.
Nonzero execution is required. Native Agent400PASS/4existingignored (9.70s),
research crate17PASS (0.03s), R7 six direct tests0.75s after build. The initial
compatible Agent rebuild took1m53s; compilation is reported separately.
Terminal owner/trace7tests took0.19s; public dispatch6tests took0.22s after build.
Clippy workspace/all-targets-Dwarnings, fmt, K10 self-test/purity, SQL and product
guards pass. Worker check11testfiles pass, including preservation
of native candidate progress for running/interrupted/completed jobs; tools/list
remains24,567bytes. Worker production routing/schema is unchanged.

These packets add no financial policy/ledger/controller and change no recorded
GET budgets/TTL/deadlines. T1/D1/R6/C20/L1 remain reuse. Whole S1-05, terminal
RELEASE lost-reply/reopen, remaining public action coverage, other diagnostic
families, full MCP R-UX/installed/performance and final integration remain open.
Historical planned assignments below are superseded by these completed rows
and #501's current remaining acceptance; do not implement them again.

### Combined diagnostics / trade contract packet after checkpoint08c

This packet continues the accepted08c source; its exact committed SHA and
full26/CI/bundle acceptance are maintained in #501's current checkpoint.
The older08c PASS does not certify a descendant. Changes are scoped as follows:

| Boundary | Actual production test / filter | Proof and remaining limits |
|---|---|---|
| Terminal INSPECT recovery | `terminal_release_reopen` (3tests) | Original catalog/SQL is closed and reopened with a real immutable zero-fill settlement. Scripted INSPECT verifies native orchestration: wrong/missing reservation/policy/sequence, denied/wrong-command/lost reply never acknowledge SQL; old session rejected; frozen old policy remains valid after current policy rotation. Repeated/concurrent Released inspection preserves the first SQL ACK and money; a real late fee stays pending. This is not the full Native+WSS+Worker lost-terminal-RELEASE bridge. |
| Observed release diagnosis | Existing stale fills/bills tests under `terminal_release` | Actual ownerâpublic Diagnostics while SQL/source locks held; exact observed frontiers/generation/order time/Worker sequence and caller ID, first-blocker context, no explain-caused effects. No reconstruction of an unobserved caller or historical venue ACK. |
| Restart diagnostic seed | `terminal_diagnostics` in Execution (3tests) and `terminal_release_trace` | Existing indexed SQL query caps candidates before scope/plan/policy joins; pending/released/late facts and matching local ACK timestamp survive reopen. Agent startup restores bounded facts; live attempts win. Unknown fields stay null. SQL state seeding in execution corruption tests is test-only; no production migration/writer/financial rule changed. |
| Worker committed RELEASE inspection | `risk_authority.test.ts`, `terminal_released_inspect_reopen_read_only` | Real Worker handler/storage settles and releases, serializes and reopens the store, rotates governance, then INSPECTs frozen policy without mutation; independent consumed0.5/reserved4/sequence7 and wrong-plan/missing-key denials. Native source claims are explicit fixtures; not a live Native handoff or physical exchange proof. |
| Public Strategies | `strategy_contract_tests` (3tests) | Actual dispatcher/registry/SQL for Create/Update/Start/Stop/List/Status/CapitalAssign, replay/conflict/stale CAS/paused authority and immutable versions. Public old-key replay after Update preserves a SQL-prepared original modern semantic intent/hash/plan/deadline; no extra venue/risk effects. It does not claim an originally accepted exchange POST or complete public new-key admission after strategy edit; unknown-account rejection is not connected-account isolation. |

The wrong-reservation defect was reproduced at the actual release owner:
`terminal_release_reopen_rejects_unbound_inspection_without_sql_ack` returned
success for a foreign key (RED0.25s). Native binding checks fix that boundary;
the receipt's frozen policy is compared, not the latest governance revision.
An earlier failed fixture (`ZERO_ENTRY_ADMISSION_MISSING`) was repaired through
the existing quote/admission owner and is not counted as that production RED.

Local Agent407PASS/0FAIL/4existingignored10.38s; Execution169PASS/2existingignored
5.33s; Worker11files PASS, including
the real Worker terminal-inspection test; tools/list24,567bytes unchanged.
SQL diagnostic3tests PASS0.31s; workspace/all-targets Clippy-Dwarnings, fmt,
K10 self-test/purity, SQL schema and product guards PASS. Compiler time is
reported separately; direct runtime does not claim an instant rebuild.
Remaining #501 criteria and full-chain lost reply acceptance stay explicit.

### #501 revision 9: finite residual acceptance list

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
The strengthened raw-observerâStatusâSQL/Worker scenario passed with no extra
POSTs or own protection incident. Existing T10-08 protection-loss recovery and
T09.8 exact budget-denial scenarios also passed locally without changing inputs,
GET budgets, TTL, command/case deadlines or monetary assertions. Two additional
assertions retain the protection/blocker outcomes in the final case artifact.
This historical fix did not establish the cause of the earlier T09.6/.7 or
price failure. The accepted 8b5b0b0 baseline includes the later source-bound
cancel chronology/POST evidence and finite-source readiness corrections.

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
required. These rows are the remaining work list for E0âE6; do not start another
repository-wide audit or silently grow it with unrelated improvements.

| Requirement / stage | Owner and change kind | Given â When â required Then | Check and current status / dependency |
|---|---|---|---|
| R0 / E0: inventory | `AgentOperation`, `query::execute`, existing owner tables; reuse | Existing enum/crates/actions â compare call sites â no missing public/background effect owner | 14 variants / 15 crates / Worker enumerated; ownership review present. This is not per-action functional acceptance. New gaps must name one row below. |
| R1 / E1: fast diagnostic contract | `diagnostics::{diagnostics,account_diagnostics,finance_diagnostics}`, `finance_trace::readback`, `protection_trace`; refactor/test | Known facts, held SQL/trace/catalog lock or unavailable Worker â public Diagnostics â bounded scoped response with known facts or explicit gap, no explain-caused SQL/remote/effect | `cargo test --locked -p okx-agent --lib diagnostics:: -- --nocapture` reuses local tests only. **Planned:** public dispatch healthy/blocked-owner/authorization tests; NOT_PROVEN. Needs owner publication and preserved existing fields. |
| R1/R6 / E1: causal identity and restart | `query::dispatch_with_governance`, trade/source/job/host owners; reuse/refactor/test | Request/background invocation, then restart/eviction/contention â explain â real origin/stage/age and missing history, no invented ACK/parent | `cargo test --locked -p okx-agent --lib diagnostics::protection_trace:: -- --nocapture` covers cleanup only. Cross-family publication/context tests **planned**; NOT_PROVEN. No second ledger. |
| R2/R4 / E2: terminal release | `terminal_release`, `risk_bridge`, `AccountStore`; test/refactor only if required | Real SQL/census/Worker flow; stale/incomplete evidence or lost reply â production release â stage-specific pending or verified same-identity ack, no duplicate debit/release | `cargo test --locked -p okx-agent --lib trade_service::unsent_release_tests:: -- --nocapture` proves unsent only; terminal cohort composition is in full26. **Planned:** direct terminal orchestration/producer-consumer proof, NOT_PROVEN. Fake complete-census flags forbidden. |
| R3/R4/R6 / E2: public Trade and Strategies | `trade_service::{mod,plan,execute,reconcile,strategies}`, SQL owner; reuse/test, fix only reproduced defect | Each supported action and applicable replay/conflict/revision/manual-strategy case â same production dispatch â correct account/amount/effect count or rejection before effect | Existing settlement/cleanup/unsent suites and full26 are partial per-action evidence. **Planned:** missing public action and strategy lifecycle tests; NOT_PROVEN. Preserve semantic idempotency and actual owner validation. |
| R4/R5/R10 / E2: S1/S2 | `accounts::recorded_support`, source observer, semantic DTO/goldens, SQL/Worker; reuse/test | Read before/crossing cancel, fresh follow-up, invalid account/query/TTL; equivalent decimal/replay/conflict â actual consumer â preserved proof ordering, exact money, no extra POST | `cargo test --locked -p okx-agent --lib immutable_signed_snapshot_phases_follow_events_not_background_read_counts -- --nocapture` checks source scheduling, not all S1. Baseline full26 accepted. Exact S1-01..04/S2-01..08 assertion mapping and missing negatives **planned**, NOT_PROVEN. Historical S table below is not current PASS. |
| R6/R8 / E3: non-trading public routing | `query::execute`, accounts/WS/analytics/portfolio owners; reuse/test | ConnectAccount identity publication; AccountSummary missing/stale; MarketOverview fresh/degraded; search filters before limit â public dispatch â exact response/scope/quality | `cargo test --locked -p okx-agent --lib query:: -- --nocapture` covers helpers only. **Planned:** dispatch contracts also covering Market/Capabilities/Microstructure/Research/Portfolio/Scenario/Mailbox; NOT_PROVEN. No new router framework. |
| R7 / E3: pure extensions and job/artifact contracts | kernel/analysis/research, `analytics::{jobs,sources,result,executor}`; reuse/test | Existing extension, queue/cancel/restart/corrupt cursor/artifact â production owners â strict dependencies, bounded resources, deterministic output or precise failure | `python scripts/check_architecture.py --self-test` and `python scripts/check_architecture.py --k10-purity` PASS on baseline. `cargo test --locked -p okx-agent --lib analytics:: -- --nocapture` reuses C16/C17 components; `cargo test --locked -p okx-research` reuses pure tests. C13âC20 assertion/extension change-budget mapping remains NOT_PROVEN, not a rewrite request. |
| R7/R8 / E3: R-UX01..08 and analyst path | analytics registry/jobs/result, portfolio SQL projection, existing MCP; reuse/test/feature-if-missing | Fixed dataset/manifest/64 candidates/train metric/split/costs â validate/backtest/grid/result/decision/export/reopen and finance drilldown â exact provenance/counts/checksums, no research-caused trade/financial mutation | `cargo test --locked -p okx-agent --lib grid_result_includes_selected_train_test_and_equity -- --nocapture` and `cargo test --locked -p okx-agent --lib portfolio:: -- --nocapture` are component evidence only. **Planned:** redacted MCP vertical transcript and required metadata/decision feature audit; NOT_RUN. Historical expense completeness is a blocker only for the named financial gate requiring it. |
| R5/R9/R10 / E4: S1-05 and production shutdown | scenario driver/child owners, `accounts::shutdown`, research executor, host CONTROL; test/fix only reproduced leak | Success/error/timeout with saved stage receipt â bounded teardown â owned children/tasks joined/stores closed, primary failure retained, next run isolated | `cargo test --locked -p okx-agent --lib source_shutdown_tests:: -- --nocapture` proves one helper. **Planned:** outer success/error/timeout lifecycle tests with real children/Windows handles; S1-05 NOT_PROVEN. No kill of unrelated processes. |
| R9 / E4/E6: installed CONTROL | host-control/launcher/release/install owners; reuse/test | Correct or wrong hash/schema/action/instance and interrupted stop/start â actual installed matching bundle â exact health/version/ack, original financial SQL preserved | `cargo test --locked -p okx-host-control` and `cargo test --locked -p okx-host-launcher` are component commands. **Planned:** installed bundle smoke; NOT_RUN. Verified downloadable bundle is prerequisite evidence, not installed proof. |
| R10 / E5: performance and change cost | public native Diagnostics, existing CPU/SQL/source/CI owners; measure then fix measured bottleneck | Release reference host, prescribed warm/cold workloads and blocked owners â public requests â useful bounded diagnostics, K9 latency/memory/control/queue correctness | Local projection P95 0.0758ms is baseline microbench only. **Planned:** public per-family 1000-sample explain benchmark, normative K9 workloads, raw samples and CI timing breakdown; NOT_RUN. Three completed changes need measured direct-loop evidence; unavailable historical timings stay unmeasured. |
| R11 / E6: final acceptance and integration | coordinator, existing guards/required CI/PR chain/docs; reuse/test | Collected row evidence + integrated code â final required checks/bundle/installed verticals â all mandatory #501 criteria on actual accepted source/build | Baseline required CI/full26/219/90/bundle accepted only at header SHA. New slice local checks then one final required CI, artifact/hash verification and ancestry/full-diff review **planned**; NOT_RUN. #499 remains Draft; PHYSICAL/REAL separate. |

#### E0 reconciliation and assigned checks (09.11.2026, source 8b5b0b0)

The inventory above covers the 15 declared workspace members, Worker, 14 public
variants, nested mutations and background owners. The following are concrete
subtasks of the existing rows, not another roadmap. Source inspection is not a
test run. Tests listed as planned have not yet been implemented or accepted.
Good existing boundaries are reused; no production defect was demonstrated in
the terminal release or diagnostic work packets below.

| Packet / parent row | Allowed files and owner | Given / When / required Then | Check / dependencies |
|---|---|---|---|
| D1 / E1 TEST | `diagnostics.rs` test module only; existing projection and `query::dispatch` | Known cleanup event plus held real SQL/source owners â public operation Diagnostics â retained identity/stage/facts, explicit missing history, correct account scope; zero SQL/HTTP/risk effects caused by explain. Restart has only actual durable/unknown facts. | Planned public-dispatch contract. Reuse finance/storage and research public contention tests; no protocol or runtime framework changes. |
| T1 / E2 TEST | `trade_service/terminal_release/tests.rs`; actual `TradeService::release_terminal` | Real SQL reservation and terminal order, zero exposure, stale fill frontier â release then repeat â FillCoverage/FILL_COVERAGE_INCOMPLETE with exact identity, unchanged receipt, no settlement/risk/venue effects. | Planned `stale_fill_coverage_blocks_release_before_worker`; existing terminal test filter. No fabricated complete census. |
| R6 / E3 FEATURE | Existing Research protocol/Worker contract, `analytics::{mod,jobs,result}`, bounded decision module | Completed immutable result â record accepted/rejected/deferred decision â read/reopen revision with exact result/experiment refs; changed hypothesis makes new immutable revision; stale expected head/cross-job/corrupt/expired input reject. | Planned research decision tests and typed record/read actions. One existing Jobs SQLite owner and artifact store; preserve original result, retention/pin graph and zero trading effects. Shared protocol files exclusively owned by this packet. |

Other finite gaps in the parent rows remain: terminal positive/stale-bills/
lost-reply/reopen/peer-pending cases through the real census; uncovered public
Trade/Strategies actions; operation families beyond cleanup causal publication;
public Market/ConnectAccount and fresh/degraded overview; R-UX experiment-card
metadata/field errors, candidate progress, complete64-candidate MCP transcript
and complete-file export; C20 rolling mean/composition/source extension proof;
outer S1-05 child/store teardown; installed bundle smoke; public per-family
Diagnostics and K9 workloads; three independent navigation/change exercises;
and final integration/ancestry checks. These are the existing E1âE6 criteria,
not permission to add venues, controllers, registries or arbitrary strategies.

S1-04 reuses the nine T09 results in the verified baseline. S1-01..03 map to
`causal_phase_transition_tests`, `immutable_signed_phase_` and the `s1_0` filters;
actual observer/operation negatives remain distinct from fixture-unit evidence.
The helper named `s1_05_other_uid_has_its_own_immutable_signed_inventory` proves
account isolation, not normative S1-05 teardown. S2-01/02/03/06 reuse
`okx-protocol` `s2_` tests, the agent decimal/legacy-reopen `s2_` tests and shared
`contracts/trade-semantic-golden.json` Worker assertions. S2-04/07 are the exact
decimal/conflict/account assertions in T09.5; S2-08 is original SQL identity/
deadline/plan/client-order/policy preservation in T09.7. S2-05 is a separate
existing CI step with `OKX_S2_05_POLICY_CAS_PROBE=1`, not implied by ordinary
T09.5: its artifact must be read separately before asserting receipt-level
acceptance. No missing S2 implementation was established by this inventory.

Current direct command corrections: unsent release is
`cargo test --locked -p okx-agent --lib trade_service::unsent_release::tests:: -- --nocapture`.
Public query tests already exist under `query::public_dispatch_tests::`:
capabilities/mailbox, instrument filtering, account-summary source/identity and
missing-overview source. Public finance/storage contention is in
`finance_trace::tests::public_finance_diagnostic_does_not_queue_behind_sql_or_trace`;
public research contention is in
`analytics::jobs::job_diagnostics::tests::public_job_diagnostic_completes_with_known_state_while_catalog_sql_is_locked`.
These retained assertions supersede the earlier broad NOT_PROVEN descriptions
for those specific paths; they do not accept every diagnostic family or K9.

Completion uses the #501 Â§7 checklist: R0 inventory; R1 explanations/context;
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
| `T09.3` | `crates/okx-agent/src/trade_service/reconcile.rs::settle_incremental_entry` | [`cases/T09.3.mjs`](../scripts/trade-integration/cases/T09.3.mjs), `--case T09.3` | FOUND: signed partial and terminal fill/bill sources | NOT_RUN | NOT_RUN | 0.12â0.30 fill, cumulative 4.002 and no retransmission | CONNECT | `f06383e` / no CI artifact |
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
| `T10-06` | `crates/okx-execution/src/ledger/sql/intents.rs::prepare; crates/okx-execution/src/ledger/sql/intents.rs::by_idempotency` | [`cases/T10-06.mjs`](../scripts/trade-integration/cases/T10-06.mjs), `--case T10-06` | FOUND: 100 concurrent public identical-key requests | NOT_RUN | NOT_RUN | One intent/reserve and â¤1 create; changed payload conflicts | CONNECT | `f06383e` / no CI artifact |
| `T10-07` | `cloudflare/okx-mcp/src/risk_book.ts::reserveRisk; cloudflare/okx-mcp/src/risk_book.ts::releaseUnsentRisk` | [`cases/T10-07.mjs`](../scripts/trade-integration/cases/T10-07.mjs), `--case T10-07` | FOUND: Worker 3Ã4 plus Native shared-campaign prerequisite | NOT_RUN | NOT_RUN | Case intentionally FAIL: same-campaign Native source-backed 8â4 release, remaining 2â6 and no false SQL bridge | EXTEND | `f06383e` / no CI artifact |
| `T10-08` | `crates/okx-agent/src/trade_service/reconcile.rs::emergency_exit; crates/okx-agent/src/trade_service/reconcile.rs::verified_live_protection` | [`cases/T10-08.mjs`](../scripts/trade-integration/cases/T10-08.mjs), `--case T10-08` | FOUND: signed partial fill and protection first leg | NOT_RUN | NOT_RUN | Case intentionally FAIL: same-intent failed protection, late fill and bounded protective attempt | EXTEND | `f06383e` / no CI artifact |
| `T10-09` | `crates/okx-agent/src/trade_service/reconcile.rs::cancel; crates/okx-agent/src/trade_service/reconcile.rs::close` | [`cases/T10-09.mjs`](../scripts/trade-integration/cases/T10-09.mjs), `--case T10-09` | FOUND: B public Cancel ACK and persisted worker first leg | NOT_RUN | NOT_RUN | Case intentionally FAIL: same-intent post-ACK late fill, partial close and remaining protection | EXTEND | `f06383e` / no CI artifact |
| `T10-10` | `crates/okx-execution/src/ledger/sql/intents.rs::bind_late_entry_settlement; cloudflare/okx-mcp/src/risk_book.ts::settleEntrySpend` | [`cases/T10-10.mjs`](../scripts/trade-integration/cases/T10-10.mjs), `--case T10-10` | FOUND: duplicate fills/bills initial causal leg | NOT_RUN | NOT_RUN | Case intentionally FAIL: same-campaign late fee/rebate, replay revision and upper breach | EXTEND | `f06383e` / no CI artifact |
| `T10-11` | `crates/okx-execution/src/ledger/sql/intents.rs::recover_posting; crates/okx-agent/src/trade_service/submission.rs::post_once; crates/okx-agent/src/trade_service/reconcile.rs::observe_intent_order; crates/okx-agent/src/source_capture.rs::capture_account_sources` | [`cases/T10-11.mjs`](../scripts/trade-integration/cases/T10-11.mjs), `--case T10-11` | 90 connected F0âF8 kills; separate component evidence excluded from that count | COMPLETE FOR T10-11 | targeted PASS; full26 pending | Preserve exact owner/crash proofs in final required gate | REGRESSION | `a8afc8dd` / CI `42564173457`; scope below |
| `T10-12` | `cloudflare/okx-mcp/src/risk_book.ts::reserveRisk; cloudflare/okx-mcp/src/runtime_session.ts::dispatch` | [`cases/T10-12.mjs`](../scripts/trade-integration/cases/T10-12.mjs), `--case T10-12` | FOUND: Worker kill/reopen persisted DO after reserve | NOT_RUN | NOT_RUN | Case intentionally FAIL: after-settlement kill, fresh generation, Native binding/no double debit | EXTEND | `f06383e` / no CI artifact |
| `T10-13` | `crates/okx-agent/src/trade_service/reconcile.rs::close; cloudflare/okx-mcp/src/risk_authority.ts::handleNativeRiskCommand` | [`cases/T10-13.mjs`](../scripts/trade-integration/cases/T10-13.mjs), `--case T10-13` | FOUND: Worker STOP and protective close probe | NOT_RUN | NOT_RUN | Assert before-claim and post-fill pause ordering, stale reduce blocker and safe reduction | EXTEND | `f06383e` / no CI artifact |
| `T10-14` | `crates/okx-agent/src/trade_service/mod.rs::TradeService::trade; crates/okx-agent/src/trade_service/reconcile.rs::cancel` | [`cases/T10-14.mjs`](../scripts/trade-integration/cases/T10-14.mjs), `--case T10-14` | FOUND: A held SQL; B public Status/Cancel | NOT_RUN | NOT_RUN | Independent B progress under held A, then join A, 1 B cancel POST | CONNECT | `f06383e` / no CI artifact |
| `T10-15` | `crates/okx-agent/src/trade_service/execute.rs::revalidate_held_entry_quote; crates/okx-agent/src/trade_service/execute.rs::verify_post_held_entry_sources` | [`cases/T10-15.mjs`](../scripts/trade-integration/cases/T10-15.mjs), `--case T10-15` | FOUND: signed fee mutation after HELD | NOT_RUN | NOT_RUN | Check FX/spec/fee/policy changes and fixed plan binding before POST; 0 send on mismatch | EXTEND | `f06383e` / no CI artifact |

## Historical A1 baseline and evidence boundary

The b93be8e Windows failure is in the T09.4 Add/Status prefix of T09.8: `INTENT_EXPOSURE_REMAINS`, correlated with `UNATTRIBUTED_CONDITIONAL_ORDER` and `COHORT_UNRESOLVED_POST_OUTCOME`. **Root cause not proven.** For A1 capture account/intent/source generation, route+query, phase ordering, two stop IDs, send/ACK, SQL/Worker revision; separate fixture publication timing from genuine unowned exposure. Use a deterministic I/O barrier; do not relax money checks, source freshness or production deadlines. The targeted positive must end in consumed 6.003, remaining 3.997, denied 4.002 and zero extra create POST. The negative must keep held/denied for incomplete evidence. Preserve baseline artifacts before changing code.

Known deliberate non-PASS cases on `f06383e`: `T10-07`, `T10-08`, `T10-09`, `T10-10`, `T10-11`, `T10-12`. Each writes or requests a partial prerequisite and throws a precise error until the integrated oracle is complete; a component probe cannot be substituted for this step. Remaining adapters also require runner validator/evidence sufficiency review and exact-SHA Windows execution.

## Historical S1/S2 and USER checkpoint at f06383e

| Scope | Evidence already located | Exact outstanding / next verification | Status on `f06383e` |
|---|---|---|---|
| S1-01/02 | `accounts.rs::recorded_support`, `trade_integration_t09_4.rs`, `trade_integration_t09_6.rs` | Explicit pre-cancel and crossing-cancel GET barriers, stale then fresh inventory with original timestamp, source/budget accounting | NOT_PROVEN |
| S1-03 | Recorded support + transport invalid-request tests | Account/query/extra POST/missing event/expired source each independently fails with first reason and no mutation | NOT_PROVEN |
| S1-04 | Existing T09.0â.8 public suite; historical b93 CI | All nine T09 on one exact commit, no executed fail; exact source-backed budget/flat/restart | NOT_RUN |
| S1-05 | Existing Native test child lifecycle | Abort/join/close stores and preserve per-stage receipts on success/failure/timeout | NOT_PROVEN |
| S2-01/02/03/04/06/07/08 | Native semantic DTO + Worker `risk_book.ts::normalizeDecimal`, goldens, SQL replay tests | Locate exact current test filter and bind golden fixture + persisted replay to this SHA | NOT_RUN |
| S2-05 | Historical Windows policy-CAS artifact from b93 CI | Repeat actual immutable policy/strategy retry versus new-key decision on release SHA | NOT_RUN |
| R-UX01â08 / analyst / manager | `crates/okx-agent/src/analytics/*`; public MCP is the intended boundary | Run validateâbacktestâgrid/OOSâresultâdecisionâexportâreopen on one dataset and save checksum | NOT_RUN |
| Trader / INSTALLED / PHYSICAL / REAL | Existing P0 runtime, governance, release and backup owners | Only after CODE and applicable release/operational gates; REAL requires separate explicit authorization | NOT_RUN / NO-GO |

**Update rule:** refresh row evidence from the next exact SHA + CI/artifact; do not copy historical PASS across commits or turn a deliberate negative into a green gate. Required mandatory milestones remain 16â24â26 PASS with 0 executed failures. No full CI was triggered for this documentation/metadata update.
## #495 â entry settlement (bounded structural refactor only)

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

**#500 protection cleanup â accepted bounded production operation.**

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

**Causal RED â GREEN:** earlier [run #42417714196](https://github.com/iamaman11/okx/actions/runs/42417714196)
recorded `cleanup_<client-id>` SQL `POSTING` and an actual cancel attempt
for `900000002` **before the signed T09.6 cancel observation phase**. This
explained the later missing target ACK without granting resend permission.
The bounded fix aligned source/operation boundaries while preserving
independent signed pre-ACK and post-ACK observations, exact-algo POST/ACK
identity and the original durable operation. A production+fixture cause
must not be collapsed into permission to manufacture a successful readback.
The final T09.6/.7/.8 PASS plus PC-01â07 proves this bounded flow;
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

In the exact CI run the direct protection tests **PC-01â07 = 7 PASS/0 FAIL
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

## #501 â unsent release: one production boundary

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
UR-03: HELD â RELEASE_UNSENT â SQL ACK, repeat retains the first receipt.
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

## T10-11 â 90 connected crashes PASS, 2026-11-09

Code SHA `a8afc8dde375584e38e762a019bb1181f238be02`,
[Windows CI 42564173457](https://github.com/iamaman11/okx/actions/runs/42564173457),
[artifacts 13651265984](https://github.com/iamaman11/okx/actions/runs/42564173457/artifacts/13651265984),
[reviewed checkpoint](https://github.com/iamaman11/okx/issues/501#issuecomment-6732653618).

The targeted case passed in 273.158 seconds. Independently checked all 90
individual records: F0âF8 ten times each, 90 distinct original SQL roots,
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

## Historical T10-11 â 80 connected crashes, superseded by the checkpoint above

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
until every required F0âF8 boundary is proved ten times. A subsequent docs-only
commit does not represent a new tested runtime SHA.

### Recorded cancel ACK and current financial frontier

Run these short boundary tests before the full T09.6â.8 campaign:

```text
cargo test --locked -p okx-agent --lib causal_phase_transition_tests -- --nocapture
cargo test --locked -p okx-agent --lib campaign_readiness_requires_capture_of_new_source_frontier -- --nocapture
```

The cancel observer binds account, target and inventory arm; an older cancel's
ACK never authorizes its readback. Waiting for the exact ACK makes no request
and uses the original deadline. The finance test invokes real capture and SQL
owners: an uncovered frontier remains blocked until the raw source is captured.
Readiness diagnostics distinguish the saved cutoff from the requested cutoff
and expose bounded cursor lag/gap state. T09.8 publishes its existing twelve
balance replies before waiting for readiness; its fee and public-time sources
remain at the later pre-Execute boundary. No read budget, TTL or money assertion
is relaxed. These direct tests do not replace full acceptance on one commit.

The Status observation checks an append-only POST prefix and the exact
prearmed account/instrument/target cancellation across concurrent callers.
It does not attribute every account POST to the observing Status invocation.
An unarmed observation permits no mutation. Create, another actor/target,
changed body/source and a second attempt at the same cancel are failures,
including attempts rejected by the recorded transport. The global nine-POST
oracle remains mandatory. T09.6 saves `T09.6/recorded-post-effects.json` with
bounded routes, actor scopes and identity hashes; JS independently verifies
the actual eighth/ninth targets. Failure projection retains the same safe
POST delta instead of only its count.

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

# Cloudflare financial witness retirement - #510

## Current integrated Native delta (16 November 2026, not full RB acceptance)

- `accounts_native_multi_close_contract_tests`: public Close 0.1 actually fills
  0.04 and cancels; a second Close uses the remaining 0.06. Independent bills
  prove exit fees 0.0002 + 0.0003 exactly once. Original terminal canceled Close
  can retire its local receipt only with its actual positive fill, scoped bill
  and flat proof. The former `filled == planned` condition was reproduced RED;
  production now requires `0 < filled <= planned`, retaining exact source sums.
  `risk_book_contract_local_partial_close_requires_terminal_positive_exact_fill_and_own_bill`
  denies zero, overfill, mismatched sum, foreign bill and nonterminal evidence.
- `accounts_native_public_cohort_contract_tests`: public A reaches original
  HELD while B performs the whole A/B census and is denied by shared cap1.5.
  A continues its original single send; exact remaining capacity is0.4995.
  The reproduced `COHORT_UNRESOLVED_RESERVE_ATTEMPT` was fixed by recording the
  actual Native owner's explicit Denied as a scoped no-send rejection. Lost
  replies remain UNKNOWN. This is not positive B execution during an A outage.
- `accounts_native_reserve_recovery_contract_tests`: cancel the public caller
  after actual HELD, before BeginPost; same-key replay and real cold reopen
  retain original identity/receipt/1.0005 entry reserve/0.102 loss without POST.
  Task cancellation is not SQL reply loss or an OS-crash witness.
- `accounts_native_changed_source_contract_tests`: independent changed fee
  after HELD refuses POST and retains the original obligation. No fake unsent
  proof is accepted. Successful unsent convergence/fresh replan remains missing.
- `finance_snapshot` and `storage_commands::native_backup`: stopped whole-DB
  snapshot includes committed WAL without modifying source main/WAL. Checksummed
  SQLite retains the earliest recovery cutoff; editing manifest time or taking
  another unresolved snapshot cannot advance it. Restore reads an immutable,
  possibly read-only archive into private no-clobber staging, verifies original
  schema/identity/cohort, and publishes only to an absent DB. Existing DB,
  orphan WAL/SHM, unknown entries and active owner refuse; no credentials/catalog
  are manufactured. A restored public Execute remains fenced until the separate
  original storage audit. Existing/corrupt DB replacement is not supported.
- `diagnostics::native_intent` now reads intent, protection, original reservation
  receipt and account-source generation/watermark in ONE scoped SQL snapshot.
  Stored source is not current venue truth or renewed freshness. Diagnostics
  never send, reconcile or grant authority. Full causal history and all RB14
  drills are still not established.

Local integrated Native run:25 passed/0 failed/0 ignored,28.16s after build;
storage:10 passed,0.91s. The earlier snapshot test failed on stale campaign
readiness after reconnect; its corrected setup uses the SAME production
maintenance refresh over captured rows, not a readiness marker. These targeted
results are not the complete RB01-RB16/26-property gate or p95. An earlier
Windows catalog publication sharing failure remains an unresolved environmental
or lifecycle boundary; later PASS does not establish its root cause.

## Current r7 delta (isolated Native tests, not full gate)

- `accounts_native_partial_protection_contract_tests`: actual 0.04 partial stop,
  failed protection plus independently billed late fill, retained incident/STOP
  and no speculative safety POST; partial reduce 0.04 leaves exact 0.06 stop,
  amendment ACK alone stays pending, independent GET verifies it and replay
  sends no second amendment. Cancel-ACK loss/emergency-success remain separate.
- `accounts_native_fee_correction_contract_tests`: independent late fee changes
  immutable cumulative entry revision 0.4002 -> 1.4002, exact bound breach
  0.3997, economic replay is read-only; real rebate changes cash but does not
  refund conservative spent capacity/breach; new Execute is fenced.
- `accounts_native_multiaccount_contract_tests`: same operation IDs on A/B
  retain different scoped capital receipts; actual source shutdown A permits
  public preparation B. This is not proof of trading fairness under lost A.
- `accounts_native_multiaccount_reserve_contract_tests`: source-backed typed
  competing owner commands share cap1.5: one Held1.0005 and one denied, exact
  remaining0.4995/loss0.1, original receipt/replay/cross-account refusal. Reported
  census is a reviewed input, not the full public multi-account producer path.
- `owner_financial_reply_tests` / `owner_financial_crash_tests`: fresh typed
  capital transfer, all ledger legs/source links, lost reply/caller cancellation
  after enqueue/commit and REAL OS child kill before/after commit; same-file
  recovery/second-writer refusal/replay. This does not certify Reserve/BeginPost
  lost-owner-reply or a second venue-send crash point.
- `diagnostics/native_intent.rs`: existing operation route `intent:<id>` reads
  one scoped durable snapshot without network or mutation; absent current
  venue/receipt evidence is explicit. Tests cover actual UNKNOWN, A/B scope and
  same readback after a real OS kill/reconnect; no full causal-history claim.
- Native canonical schema v8 installs immutable fee-owner/late-settlement guards
  at initialization. Runtime fee ingestion no longer mutates that schema. v7 is
  refused without migration/reset; installed data was not opened or changed.
- Pure coverage self-tests reject missing/duplicate/foreign/skipped results and
  distinguish full DECLARED manifest from subset. They are NOT an executing
  complete gate or proof that its declared manifest covers every invariant.

Current local validation: Native20 PASS/0FAIL/0ignored in21.88s; complete
execution337 PASS/0FAIL/2 historical ignored probes in10.53s, separately timed.
Mandatory full coverage, public Add/remaining source variants, post-release
corrections, trading fairness, current backup/restore and installed lifecycle
remain NOT_PROVEN. Historical tables below are preserved audit snapshots,
superseded only by the exact properties of these newer executed witnesses.

Supported execution remains OKX linear USDT swaps, with a32-account census cap
and bounded owner queue128. Neither universal market support nor unlimited
accounts/trades follows from these PASS results.

Cloudflare owns authentication, MCP, transport and governance configuration. Rust Engine and its single `finance.sqlite` owner enforce financial constraints. This changes the witness boundary, not the financial invariants of #449. Old DO financial data is neither read as authority, migrated, reset, nor deleted by the current runtime.

`npm run check` retains auth, typed schemas, MCP, governance configuration CAS/idempotency, runtime dispatch, backpressure, queue/close priority and shared numeric/semantic checks. `node scripts/test-native-finance-retirement-do.mjs` additionally runs actual workerd/SQLite DO transport: token authentication, session/generation fencing, typed obsolete RPC denial, liveness, dispatch ACK/response, and persisted transport generation after restart. Neither command certifies a Native full trading lifecycle.

The historical files below are **RETIRED, not PASS**. They must not count as proof of a current financial path. Original Native/Worker crash scenarios are retired when their Cloudflare finance transaction boundary disappears. #510 must map current CI scenarios to Native owner witnesses; the active Worker CI now checks the transport-only boundary and Rust component invariants. #512 is subsequent measured optimization. The complete Windows matrix remains unadapted/NOT_PROVEN, so this does not authorize GO.

| Previous witness | Status and retained invariant | Current Rust boundary / replacement witness |
| --- | --- | --- |
| `src/risk_book.test.ts` reserve/shared ceilings/high water, campaign and account constraints | RETIRE Worker financial owner; retain exact amounts, CAS, denied operations never create credit | `UnifiedFinanceHandle::reserve_reported` (`native_admission.rs`) to `risk::reserve_unified_native`; `risk_book_contract_scoped_original_reserve_shared_budget_and_high_water_on_denial` and unified owner tests |
| Old `src/risk_authority.test.ts` reserve / causal BeginPost | RETIRE old RPC acceptance; retain source freshness and one durable pre-send decision | `UnifiedFinanceHandle::begin_post_reported`, `NativePostDecision`, `CommittedVenueSend` to `risk::begin_post_unified_native` |
| Old `src/risk_authority.test.ts` settle / terminal release / unsent release | RETIRE DO mutation assertions; retain exact once settlement, proven terminal and unsent release, no premature credit | `entry_settlement.rs`, `sources/original_bound_entry_settlement_sql` to `risk::settle_entry_unified_native`; `resolve_terminal_status` / `release_terminal_status` (`terminal_status.rs`) to `risk::resolve_terminal_unified_native`; `release_unsent_status` (`unsent_status.rs`) |
| Financial portions of `src/account_governance_store.test.ts` | RETIRE coupling configuration acceptance to old DO consumption; KEEP validation, monotonic configuration, revision CAS, receipt idempotency and atomic rollback | `UnifiedFinanceHandle::apply_campaign_policy`; `source_campaign_initialization.rs` against original risk rows; native admission enforces effective budgets |
| `src/legacy_finance_cutover.test.ts`, `scripts/test-legacy-finance-cutover.mjs` | RETIRE optional marker/cutover migration as an activation requirement; no current deployment or persisted data mutation | Production module graph excludes `legacy_finance_cutover.ts`; configuration tests assert old keys untouched and unread |
| `src/acceptance_composed.test.ts` | RETIRE central RiskBook / fake venue financial composed witness; retain MCP authentication/typed routing, no fabricated Native claim | `accounts_native_trade_contract_tests` actual public application chain; ordinary full closed lifecycle passed locally; negative and physical variants remain separately NOT_PROVEN |
| `scripts/test-runtime-do.mjs`, `scripts/t10-11-native-crash*.mjs`, `scripts/t10-12-*.mjs`, `src/integration/runtime_crash_entry.ts` financial scenarios | RETIRE old actual Native + Worker financial/Worker-crash scenarios; preserve normative ambiguity/restart/settlement invariants | `posting_recovery.rs`, `owner_startup::open_native`, original-row unsent and terminal owner tests; current workerd transport witness is separate and does not certify these finance scenarios |
| Current `src/risk_authority.test.ts` | KEEP distinct retirement contract: exact `NATIVE_FINANCE_AUTHORITY_REQUIRED`, no storage access, no budgets, valid native session, stale/wrong session fenced | Current production adapter and actual workerd test, with no financial authority claim |

Historical `risk_book.ts` and `legacy_finance_cutover.ts` remain only to compile retained historical evidence. `account_governance.ts` and `execution_tools.ts` use `decimal.ts`, which contains pure numeric contract helpers and no storage. The current esbuild transport test asserts that neither historical financial authority module occurs in the actual runtime module graph. The retained `risk_book_sequence` and optional budget fields are typed legacy wire contracts only; the active adapter always returns a null sequence and no budgets.


## #510 prerequisite: current 26-property map (code audit, not acceptance)

#510 is not GO while the required full gate invokes removed Worker financial authority or lacks equivalent current Native witnesses. Adapting that gate is part of #510, not a deferred #512 obligation. The 26 IDs and normative properties below are retained from `scripts/run-trade-integration.mjs` and its current case files. Every old case uses `actualNativeWorker` / the historical Worker finance driver; no historical 26-case result transfers to the new model. `REPLACE / NOT_PROVEN` means the driver must change and this audit supplies no whole-case PASS. `KEEP component` means retain the named narrow test, not promote its result to case acceptance. No Rust test was executed during this mapping audit.

Exact inspected symbol locations: `N` = `crates/okx-agent/src/accounts_native_trade_contract_tests.rs`; `F` = `crates/okx-agent/src/accounts_native_filled_close_contract_tests.rs`; `U` = `crates/okx-execution/src/account_worker/unified/`; `A` = `crates/okx-execution/src/account_store_tests.rs`; `R` = `crates/okx-execution/src/risk/book/tests.rs`; `E` = `crates/okx-agent/src/trade_service/execute.rs`; `W` = `crates/okx-agent/src/cloudflare_transport.rs`. These are inspected existing implementations, not proposed test names. The fresh Native fixture (`accounts_native_trade_fixture.rs`) calls `UnifiedFinanceHandle::initialize_new`, `AccountRuntimeRegistry::open_native_catalog` and `NativeRiskClient::local(NativeFinanceRisk::from_owner(...))`. In contrast, imported/original-row SQL fixtures and old per-account AccountStore tests do not establish the fresh Native producer or physical process boundary.

| Required ID | Preserved property | Existing exact Rust witness and its actual boundary; remaining obligation | Disposition |
| --- | --- | --- | --- |
| T09.0 | Two verified accounts, source capture, original campaign opening, shared USD 10 ceiling; no permission inferred from a zero balance | `U/owner_startup_tests.rs::risk_book_contract_new_owner_exact_replay_reopen_and_no_trade_permission` uses initialize_new/open_native, rejects a second writer and absent bootstrap. `U/account_initialization/tests.rs::risk_book_contract_native_account_initialization_scoped_replay_and_reopen` and `U/campaign_initialization/tests.rs::risk_book_contract_native_campaign_policy_two_accounts_replay_and_no_opening_grant` cover scoped accounts/policy, not a full two-account signed opening transcript. `N::risk_book_contract_native_maintenance_installs_original_opening_before_market_admission` uses actual source maintenance on fresh owner. Recompose these into the case. | REPLACE / NOT_PROVEN; KEEP components |
| T09.1 | Public Execute, original reserve/BeginPost/send, signed fill/protection, exact entry 1.0005, terminal settlement/release | `F::risk_book_contract_native_filled_close_settlement_release_and_cold_reopen` exercises fresh owner public Execute/Status/Close, signed fills/bills, exact 1.0005 and original release/readback. Its source exists; this audit does not certify its current execution or every original case assertion. | REPLACE / NOT_PROVEN; candidate current Native lifecycle |
| T09.2 | Second public admission only after the first source-backed debit; one shared remaining campaign budget | `N::risk_book_contract_native_public_execute_lost_ack_and_replay_send_once` covers one original admission; `F::risk_book_contract_native_filled_close_settlement_release_and_cold_reopen` covers its debit. Neither inspected fresh-owner test performs the case's second public admission after that debit. `A::e1_e2_native_real_fee_sources_baseline_and_second_admission_under_ten` is old AccountStore component evidence only. | REPLACE / NOT_PROVEN |
| T09.3 | Incremental partial fills, exact cumulative financial debit, proportional protection and no duplicate charge | `A::risk_book_contract_unified_original_unknown_partial_ack_has_exact_source_and_replay` checks original unified SQL UNKNOWN/partial outcomes and replay with explicitly handwritten SQL source fixtures. `F::risk_book_contract_native_filled_close_settlement_release_and_cold_reopen` uses full fill, not incremental partial protection. Fresh-owner public partial sequence remains required. | REPLACE / NOT_PROVEN; KEEP SQL component |
| T09.4 | Managed add uses original intent/lot ownership, same campaign, newly verified fill/protection, bounded aggregate exposure | `F::risk_book_contract_native_filled_close_settlement_release_and_cold_reopen` proves one managed lot and close, but no public managed-add scenario. `E::e2_managed_exposure_is_external_zero_only_after_exact_ownership_proof` retains pure ownership-proof negatives. No inspected current fresh-owner full add replacement. | REPLACE / NOT_PROVEN; KEEP ownership component |
| T09.5 | Same semantic request/key replays one original intent, changed payload conflicts, governance CAS cannot rewrite signed identity/hash/deadline | `N::risk_book_contract_native_public_execute_lost_ack_and_replay_send_once` asserts one venue Create across Execute replay and Status. `okx-protocol/src/trading.rs::s2_01_execute_semantic_decimal_equivalence` and `s2_06_same_golden_preimage_and_sha256_for_reordered_semantics` retain wire identity. Fresh-owner governance-CAS interleaving and altered payload are not supplied by that single admission test. | REPLACE / NOT_PROVEN; KEEP semantic/replay components |
| T09.6 | Two distinct public closes 0.30 and 0.20, exact seven bills/fees, no residual lots/algos, terminal source-backed release | `F::risk_book_contract_native_filled_close_settlement_release_and_cold_reopen` proves one close with original sources, no residual risk and cold reopen. It does not establish the original two-close/seven-independent-bill economics. Those exact sources and quantities remain required. | REPLACE / NOT_PROVEN |
| T09.7 | Restart preserves terminal lots, immutable fees, cumulative campaign debit and released reservations; no reconstruction credit | `F::risk_book_contract_native_filled_close_settlement_release_and_cold_reopen` reads original filled/released intents and financial totals after close/reopen. `U/posting_recovery.rs::original_joint_recovery_survives_sqlite_close_and_reopen_without_new_grant` uses original-row fixture and retains unknown budget. Neither is physical OS process kill; do not relabel graceful reopen as such. | REPLACE / NOT_PROVEN; KEEP reopen components |
| T09.8 | After exact cumulative 6.003 spend, deny requested 4.002 against 3.997 remaining, zero new POST, unchanged debit after reopen | `R::risk_book_contract_scoped_original_reserve_shared_budget_and_high_water_on_denial` tests unified SQL cap/high water with test-only inserted original rows. `F::risk_book_contract_native_filled_close_settlement_release_and_cold_reopen` proves 1.0005, not the normative 6.003/3.997/4.002 chain. Fresh Native signed-source cap-denial sequence remains required. | REPLACE / NOT_PROVEN; KEEP money component |
| T09-MA | Concurrent A Status and B positive Execute, same instrument and scoped strategy, cross-account mutation denied, one shared campaign | `accounts_native_strategy_contract_tests.rs::risk_book_contract_native_public_strategies_crud_claim_no_trade_share_original_owner` proves scoped public strategy operations on fresh owner but explicitly no trade. `A::two_account_100_parallel_sql_readonly_isolation` uses old separate AccountStore owners. No current fresh single-owner two-account public barrier/positive Execute witness inspected. | REPLACE / NOT_PROVEN; KEEP strategy component |
| T09-SPEC | Second authorized linear spec keeps exact multiplier/tick/lot/minimum, rejects missing permission/off-grid/sub-minimum sizing, one valid POST | `trade_service/plan.rs::b_second_linear_contract_spec_uses_existing_precision_gate` checks ETH rules/independent precision goldens. `N::risk_book_contract_native_public_execute_lost_ack_and_replay_send_once` only executes BTC. Current fresh-owner second-spec public execution and permission denial remain required. | REPLACE / NOT_PROVEN; KEEP precision component |
| T10-01 | Missing/stale sources before admission and after HELD deny with no send; fresh signed sources recover positively | `U/native_admission.rs::native_census_rejects_missing_stale_incomplete_or_pre_floor_nav` and `native_census_accepts_original_financial_and_policy_sources_without_writes` check admission verification against SQL fixtures. `N::risk_book_contract_native_capture_and_public_execute_require_original_opening` checks missing original opening on fresh public Execute. Full stale-after-HELD then positive recovery source transcript remains required. | REPLACE / NOT_PROVEN; KEEP verifier components |
| T10-02 | Exact managed ownership can admit; external/unowned or unknown exposure cannot be guessed flat; budgets unchanged on denial | `U/native_admission.rs::native_census_rejects_wrong_loss_foreign_roster_or_unproven_external_occupancy` verifies wrong losses/roster/unproven external occupancy. `E::e2_managed_exposure_is_external_zero_only_after_exact_ownership_proof` verifies exposure classification. Both are verifier components; no current fresh public positive/negative exposure transcript. | REPLACE / NOT_PROVEN; KEEP verifier components |
| T10-03 | Reply lost after committed reserve/BeginPost is reconciled by exact original identity; no duplicate grant/send or invented release | `N::risk_book_contract_native_public_execute_lost_ack_and_replay_send_once` is venue ACK loss, not owner reserve reply loss. `R::risk_book_contract_sql_ack_lost_replay_is_read_only` uses old RiskBookStore. Fresh UnifiedFinanceHandle committed-command response-loss/reconcile witness is absent from inspected public tests. | REPLACE / NOT_PROVEN |
| T10-04 | Foreign/misbound reply never authorizes another intent/account/generation; no post and no fabricated release | `W::foreign_risk_reply_command_never_authorizes_pending_reserve`, `stale_native_risk_reply_cannot_grant_a_different_generation`, `same_risk_command_id_with_different_payload_never_inherits_worker_result` exercise actual Rust serde/session callbacks over scripted streams. KEEP callback contracts; current Native finance no longer asks Worker to grant. Full new-owner local receipt identity/no-send adaptation remains required. | REPLACE / NOT_PROVEN; KEEP typed callback tests |
| T10-05 | A transmitted order with lost/unbound ACK stays UNKNOWN without resubmit; exact client-order readback; explicit rejection distinguished | `N::risk_book_contract_native_public_execute_lost_ack_and_replay_send_once` uses fresh owner/public send, deliberately consumes lost create ACK, asserts UNKNOWN and one Create through replay/Status. `E::t10_venue_post_ack_requires_exact_order_binding_and_unambiguous_result` keeps ACK classification. Fresh-owner explicit venue reject branch still needs a complete recorded case witness. | REPLACE / NOT_PROVEN; KEEP current ACK/replay components |
| T10-06 | 100 concurrent public requests with one semantic key converge to one intent or proven no-send; one venue Create; altered payload conflicts | `W::identical_inflight_risk_commands_get_one_worker_call_and_two_bound_replies` is callback coalescing, not public Execute concurrency. `N::risk_book_contract_native_public_execute_lost_ack_and_replay_send_once` replays sequentially. `E::duplicate_execute_one_post` uses old AccountStoreHandle fixture. Current fresh-owner public 100-request barrier remains required. | REPLACE / NOT_PROVEN; KEEP callback/replay components |
| T10-07 | Three competing A/B reserves share one campaign: HELD, HELD, DENIED; source-backed release creates only exact available capacity | `R::risk_book_contract_scoped_original_reserve_shared_budget_and_high_water_on_denial` checks original unified SQL reserve/caps/high water with handwritten fixture rows. `U/campaign_initialization/tests.rs::risk_book_contract_native_campaign_policy_rejects_bad_limits_roster_and_refill_atomically` keeps fresh-owner policy no-refill validation. No inspected fresh-owner three-public-request/source-backed release witness. Old Worker shared-cap component is retired. | REPLACE / NOT_PROVEN; KEEP SQL/policy components |
| T10-08 | Partial fill plus protection failure fences new risk, permits bounded protection action, and reconciles late-fill money | `F::risk_book_contract_native_filled_close_settlement_release_and_cold_reopen` covers ordinary filled lifecycle and cleanup, not partial protection failure/late fill. `E::t09_native_post_once_partial_fee_terminal_campaign_under_ten` uses older AccountStore orchestration fixtures, not fresh owner. No current full replacement inspected. | REPLACE / NOT_PROVEN |
| T10-09 | Cancel ACK does not release risk; late fill plus partial close preserves remaining lot/protection and exact money | `F::risk_book_contract_native_filled_close_settlement_release_and_cold_reopen` performs full close; no cancel-ACK/late-fill/partial-close sequence. `A::k6_v02_sqlite_partial_fills_close_fees_funding_and_replay` retains old AccountStore money mechanics only. Current fresh Native sequence remains required. | REPLACE / NOT_PROVEN; KEEP money component |
| T10-10 | Duplicate fill/bill is one economic debit; late fee/rebate and settlement revisions replay exactly; bound breach remains fenced/pending | `A::risk_book_contract_unified_original_unknown_partial_ack_has_exact_source_and_replay` uses handwritten original unified SQL facts. `A::p02_m2_finance_reversal_is_exact_one_owner_and_idempotent_after_restart` is old AccountStore economics. Current signed-source fresh-owner duplicate/late fee/breach public lifecycle is not supplied by those components or the one-filled-close test. | REPLACE / NOT_PROVEN; KEEP SQL economics components |
| T10-11 | Historical witness: OS child kill F0-F8, ten repetitions each. Preserve distinct current durable boundaries, fresh reopen, no duplicate POST/finance and ambiguity; #510 r5 does not require obsolete repetition counts | `E::k5_windows_actual_process_kill_native_component_phases_ten_times_each` launches old AccountStoreHandle fixtures. `A::risk_book_contract_unified_original_begin_post_and_account_intent_one_commit` kills child at before/after transaction commit through `risk_book_contract_child_unified_native_post_os_kill_barrier`, but uses imported original fixtures/test_stage helper, not initialize_new production owner. Fresh Native `risk_book_contract_native_os_kill_after_create_recovers_original_unknown` now proves actual child death after one Create before outcome commit, money/fence preservation, UNKNOWN and reconnected public Status without POST. Other current durable crash boundaries remain NOT_PROVEN. | REPLACE / NOT_PROVEN; KEEP narrow old/imported components only |
| T10-12 | Worker OS process death/reconnect cannot grant, duplicate finance or lose original owner state; stale replies rejected, Native process survives | `U/posting_recovery.rs::original_joint_recovery_survives_sqlite_close_and_reopen_without_new_grant` is fixture SQL graceful reopen, not physical Worker death. New `test-native-finance-retirement-do.mjs` proves real workerd dispose/restart and stale generation denial, not OS kill. Financial commit moved to Native; retain transport OS-child-kill/no-duplicate Native effect requirement with a new physical witness. | REPLACE / NOT_PROVEN; KEEP narrow recovery/transport components |
| T10-13 | STOP after filled exposure blocks new claims but permits source-valid reduce/close; stale reductions stay blocked and unrelated reservation remains | `U/campaign_initialization/tests.rs::risk_book_contract_native_campaign_policy_two_accounts_replay_and_no_opening_grant` checks revised policy; `F::risk_book_contract_native_stop_after_fill_blocks_open_but_closes_and_releases` now proves STOP after fill, new Open denial/no mutation, successful source-verified Close/release/reopen. `E::close_under_stop` is old AccountStore orchestration fixture. Independent second-account reservation preservation and stale reduction negatives remain required; do not label the complete ID PASS from the one-account positive case. | REPLACE / NOT_PROVEN; KEEP policy/close components |
| T10-14 | Fairness: held A job does not starve B public Status and exact Cancel; same original single owner retains unrelated budgets and rejoins | `A::blocked_a_sql_owner_does_not_delay_b_cancel_receipt` and `two_account_100_parallel_sql_readonly_isolation` deliberately use separate legacy AccountStore owners. They do NOT establish fairness on the new single UnifiedFinanceHandle writer. Existing workerd backpressure test covers transport priority only. Fresh `risk_book_contract_fresh_owner_queue*` now proves capacity128, FIFO drain, deadline/caller cancellation and coherent reopen. Public B Cancel/Status under a blocked A source and preservation of both budgets remain required; queue markers do not prove financial fairness. | REPLACE / NOT_PROVEN; KEEP separate-owner/transport components only |
| T10-15 | Signed fee/spec source changed after HELD rejects stale plan/no post, durable exact unsent release, fresh replan positively validated | `E::e1_held_entry_sources_reject_tampering_and_stale_tariffs` and `trade_service/plan.rs::e1_changed_venue_rules_never_reuse_original_entry_plan` keep verifier negatives. `A::risk_book_contract_unified_bound_unsent_reads_original_sql_without_fake_no_send` verifies release from original but handwritten SQL facts. Fresh initialize_new public changed-source/unsent-release/replan chain remains required. | REPLACE / NOT_PROVEN; KEEP verifier/release components |

Current added fresh-owner evidence: public filled/protection/Close/settlement/release/cold-reopen contract passed locally (14.35s), and real new-Native child kill after one Create followed by UNKNOWN/reconnect/public Status passed (6.51s). These replace only their actual narrow properties; the full per-ID matrix above remains honest about partial fills, STOP, fairness and other crash boundaries.

Additional narrow accepted contracts: fresh-owner STOP after fill rejects new
Open without financial mutation and permits the same source-verified Close and
release; partial-source missing-bill rejection, late bill0.4002, second fill1.0005
and exact replay retain active exposure. Partial attached-stop sizing and
independent second-account obligations are not established by these witnesses.

## #510 r6.1 delta: public Execute safety

`accounts_native_execute_safety_contract_tests.rs`, under the existing
`native_trade_contracts` filter, adds four current fresh-owner witnesses:

- T10-05 / D09: a bound venue rejection persists REJECTED, retains its actual
  order-level `sCode` (51008, not the successful envelope's code 0), sends once
  and does not release the reservation merely from that ACK.
- The same bound rejection is checked with an all-failed envelope (code 1).
  A successful row inside that failed envelope is never accepted; foreign
  binding remains UNKNOWN, as covered by the direct ACK classification unit.
- T10-05 / D09: a raw foreign rejection reaches the PRODUCTION ACK verifier,
  remains UNKNOWN with no bound venue order, and retains the reservation. The
  recorded HTTP guard does not substitute its own transport error for this
  negative witness: the local delivery boundary corrupts exactly one raw ACK.
- T10-06 / D06: 100 public same-key callers start at one barrier. One real
  planner pauses before preparation; the other 99 return ACCOUNT_MUTATION_BUSY
  with no POST. Resuming the planner sends once; a lost ACK leaves the original
  intent UNKNOWN. Later policy delivery cannot rewrite the persisted intent;
  stale/conflicting policy revisions fail. An altered payload conflicts without
  changing its budget.

All 13 current `native_trade_contracts` tests PASS together locally in 15.09s
after build, including the existing lost-ACK/replay/readback, full Close/reopen,
STOP, partial-fee and actual child-kill contracts. No new CI filter is needed.
This is neither the full 26-property gate nor a benchmark of all RB properties.
The table above is the historical mapping audit; these delta witnesses extend
its previously missing rejection/concurrency/policy-CAS coverage. Three fresh
same-binary runs also PASS (15.31s / 15.44s / 15.16s); complete execution is
332 PASS / 0 FAIL / 2 historical ignored probes (10.89s), separately timed.
An earlier intermittent child/STOP census failure is not declared fixed by
these repeats. Child failure output is retained; terminal census now preserves
its specific failed temporal boundary. Source ordering/repeatability and other
NOT_PROVEN full-gate obligations remain outstanding.

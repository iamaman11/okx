# #518 — T01–T15 evidence matrix (CODE, not release acceptance)

**Evidence cutoff:** 21 November 2026 (Europe/Minsk). Canonical repository:
[`iamaman11/okx`](https://github.com/iamaman11/okx). Last completed integrated
Draft baseline: [`5e4a83728bd77ab0a9bea0fe18bd405a9c998317`](https://github.com/iamaman11/okx/commit/5e4a83728bd77ab0a9bea0fe18bd405a9c998317),
tree `68072635acf81a1256a0e93a1ed0d3b27a163ae8`,
[CI #4728](https://github.com/iamaman11/okx/actions/runs/44850412915)
**SUCCESS**: classify, dependency-policy and selected Draft feedback, including
100 real loopback MCP HTTP → SQLite Durable Object → authenticated WebSocket →
Native Finance calls under two real CPU Grid jobs (p95 44.019 ms), 600 real
synthetic-source IPC requests per phase and 1,014 process-tree RSS observations.
The next tree adds real IPC boundary rejections and sampled process I/O;
use the current [Draft PR #529](https://github.com/iamaman11/okx/pull/529)
for its exact-tree CI result. Full Native Windows remained **SKIPPED** for Draft.

## How to interpret a row

- `PASS` means the named **CODE** contract and negative controls are covered
  by source inspection and the cited executed Draft tests. It never means
  INSTALLED, PHYSICAL, REAL, final T01–T15 acceptance or trading permission.
- `BLOCKED` means a required observed proof is absent; this is not a waiver.
- `NOT_RUN` means the required campaign has not run on this exact executable
  tree. Inherited evidence from an older accepted release is not a substitute
  after changes to shared Kernel/Protocol code.
- External capability `unavailable` is not automatically a test failure:
  several T06/T09 criteria **require** explicit unknown/unsupported outcomes
  instead of fabricated data or financial conclusions.

The integrated final T01–T15 acceptance remains **NOT_RUN**. The following
table records the narrower, reproducible CODE state and the exact next proof.

| Criterion | CODE | Direct executable witness / public boundary | Remaining qualification |
|---|---|---|---|
| **T01 — catalog** | **PASS** | `okx-research-worker::analytics::tests::market_capabilities_discloses_supported_operator_names_and_units`; MCP `execution_tools.test.ts`: 16 distinct IDs, valid/invalid ID, strict 14 accepted and 15/16 rejected, parity with Native descriptors. | Verify installed MCP/Worker generations after an accepted release; an on-demand catalog is not historical-source availability. |
| **T02 — units and shape** | **PASS** | `okx-kernel::query_plan::t02_scalar_window_and_contract_currency_mix_fail_before_sources`, `t02_t03_t13_serialized_checked_plan_roundtrips_only_after_recompilation`; `okx-research::economics::t02_t09_linear_contract_conversion_is_typed_and_shorts_reverse_gross`; Worker rejects OI/funding composition on spot before reads. | Contract conversion is tied to an explicitly supplied/current instrument specification, not a verified historical spec schedule. |
| **T03 — step identity** | **PASS** | `okx-kernel::query_plan::t03_checked_composition_allows_branching_and_repeated_ops_but_rejects_bad_refs`; Worker `t02_t03_public_row_uses_checked_operator_and_parameter_binding`; no second graph interpreter or source owner. | A different operator ID/parameter means a different typed plan; no arbitrary unchecked JSON execution. |
| **T04 — grid and quality** | **PASS** | `okx-kernel::time_series::t04_exact_five_bar_grid_distinguishes_missing_duplicate_and_right_boundary`; Worker strict candle/OI coverage; real Agent→Worker `t06_real_worker_cross_instrument_oi_alignment_is_strict_and_isolated`: 100/100 versus 99/100 at +1 ms, no nearest/forward fill. | Source-specific actual historical coverage depends on the venue/archive. |
| **T05 — numeric semantics** | **PASS** | `okx-kernel::query_plan::t05_returns_series_can_feed_a_ratio_window_without_changing_scalar_returns` and `t05_t10_sliding_mean_agrees_with_independent_window_oracle_and_propagates_gaps`; Worker `t05_composition_reuses_closed_candles_and_projects_a_distinct_ratio`. | f64 statistics have specified tolerances; exact settlement cashflows use Decimal, not f64. |
| **T06 — time joins / Pearson** | **PASS** | Kernel `t06_sparse_event_grid_is_exact_and_does_not_invent_publication_time`, `t02_t06_rolling_pearson_requires_thirty_exact_pairs_and_preserves_coefficient_type`; Worker `t06_pearson_needs_thirty_known_aligned_pairs_not_future_or_shifted_data`; real child IPC causality + two-instrument alignment tests. | **Data capability unavailable:** historical publication timestamps of OI/funding are unverified, current ticker is not a historical spread; decision signals relying on unknown publication time remain `null`. |
| **T07 — event / label** | **PASS** | `okx-kernel::query_plan::t07_outcome_cannot_be_a_feature_or_event_condition`, prefix-invariance / historical `evaluate_at` checks; Worker `t07_event_graph_exposes_boolean_metrics_but_not_unconfirmed_future_outcomes`; Agent child `t07_t08_real_worker_intraday_event_and_holdout_stay_separate` on 1,440 bars. | Public `market.screen` projects the **last closed bar**; its unconfirmed future label is `null`. Historical `evaluate_at` is a pure Rust operation, not a live trading signal or a public batch-label export. |
| **T08 — strategy / OOS** | **PASS** | `okx-research::strategy_registry::breakout_grid_sixty_four_train_only_candidates_and_oos_isolation`; Worker `t08_public_breakout_grid_persists_typed_selection_and_rejects_65_before_sources`; Agent child 1,440-bar Grid: identical 1,008-bar train, changed 432-bar holdout, stable selected variant and candidate train hashes. | `close_breakout_v1` is a counterfactual Research definition; do not infer live strategy activation, authority or profitability. |
| **T09 — economics** | **PASS** | `okx-research::economics::t09_three_linear_contracts_exact_oracle`; `economic_audit` replays fills/fees/funding/equity, cross-currency cashflow buckets remain separate; Agent child `t06_t09_real_worker_causality_and_economics_are_isolated`: +3 USDT and +2 USD, no FX/joint risk. | **Data/model capability unavailable:** `historical_observed` refuses with `HISTORICAL_RULES_AND_FEES_UNAVAILABLE`; no historical fees, rule schedule, dated FX portfolio valuation, venue mark or option model is implied. |
| **T10 — resource limits** | **BLOCKED** | Direct bounds tests cover 1M/1M+1 bars, 8/9 instruments, 64/65 variants, graph cells/depth, 4,096 MiB shared *estimated* permit and two heavy slots. Draft [#4728](https://github.com/iamaman11/okx/actions/runs/44850412915) measured 100 end-to-end MCP→DO→WSS→Native Finance calls under two CPU Grid jobs (p95 44.019 ms), plus direct Native Finance and cancel timing. The expanded integrated fixture counts real synthetic-source IPC requests/returned rows and samples Linux RSS of the Agent+MCP+Research process tree every 20 ms, separate from build time. The next slice also rejects 1M+1 bars, 9 instruments and 65 variants via the **real Agent→child Worker IPC** before any source request, without actually scheduling the admitted 1M-bar grid; Linux `/proc` I/O counters observe all sampled test processes without attributing bytes to a source. | Source requests are to an isolated synthetic handler, **not** venue HTTP or independently identified storage-page reads. Sampled process I/O and summed RSS may miss between-sample or short-lived activity; summed RSS double-counts shared pages and is **not** a hard memory limit. Windows Job Object measures peak committed job memory, not RSS. Need final fixed-machine campaign on exact release tree with full source/page I/O, actual peak/limit, hardware and resource saturation, true OAuth/network, physical installed Windows and complete T10 SLO. |
| **T11 — effects / rights** | **PASS** | `scripts/check_architecture.py` isolates Worker from financial dependencies; worker capabilities declare no venue POST/finance/governance writes; IPC child tests cover research metadata, no-effect economics, read-only diagnostics and cancellation under saturated command lanes. | This is CODE/process-fixture evidence; no right to execute a real TradeService command is transferred by an artifact or `RecordDecision`. |
| **T12 — financial invariants** | **NOT_RUN** | Existing accepted Finance/RiskBook property suite and prior [full four-component #527 matrix](https://github.com/iamaman11/okx/actions/runs/44638604558) give a historical baseline; targeted Engine-under-Research-load child regression passes in Draft. | Shared `okx-kernel`/`okx-protocol` changed in #529: a **fresh exact-tree** full Native/Finance Windows matrix, including preview/UNKNOWN reconciliation and 61/61 property gate, remains mandatory before release. No exchange POST or financial operation is authorized here. |
| **T13 — repeatability** | **PASS** | Worker `t13_composed_market_route_persists_input_evidence_and_replays_same_job`, `t13_export_rechecks_all_replay_dataset_pages_and_exact_content_hash`, source-page reconstruction and bounded independent reexecution; Agent child immutable cursor paging, changed OOS dataset hash, old typed result reads. | Unverified publication time is never upgraded by a matching hash; large/unsupported replay explicitly says `not_verified`. |
| **T14 — lifecycle** | **PASS** | Worker `t14_market_retry_and_cursor_never_publish_cancelled_or_interrupted_partial_result`, `t14_partial_artifact_is_never_advertised_as_a_completed_result`; child `ipc_process` lost-ACK/restart/conflicting-key/real-process controls and Agent cancel/reopen tests. | Interrupted work is explicitly terminal/diagnosable, never silently published complete. Installation handoff is a separate release acceptance. |
| **T15 — extensibility** | **PASS** | [`t15-range-extension.md`](t15-range-extension.md): new pure versioned `rolling_range`, independent oracle, checked plan, Worker descriptor, MCP allowlist and two previously unnamed user graphs through the same `market.screen` route. | This proves composition of available primitives, not automatic support of new venue adapters, FX, arbitrary custom strategy execution or TradeService dispatch. |

## Actual remaining work, in the necessary order

1. **T10 measured proof.** Run a bounded, recorded acceptance on the final
   candidate with hardware, OS, Rust/profile version and source-tree identity.
   Keep compilation, child startup, wall-clock execution, peak whole-process
   RSS, actual source reads and cancellation time separate. An estimated memory
   permit and a passing unit test are not performance acceptance.
2. **T12 exact-tree full Native/Finance proof.** Because the Draft includes
   shared Kernel/Protocol modifications, do **not** reuse the previous #527
   financial matrix or start the heavy Windows matrix on every Draft push.
   One fresh full release run is required on the frozen candidate. The owner
   decides when to mark PR Ready; do not merge or install without explicit
   authorization.
3. **Final T01–T15 integrated review.** Assemble input/output/reason, effect
   counts, source/economic assumptions and test logs on one frozen tree;
   classify each criterion PASS/FAIL/BLOCKED/NOT_RUN at CODE, INSTALLED,
   PHYSICAL and REAL levels separately. Mark genuinely unavailable external
   data as unavailable, not as an invented historical fact.
4. **Separate product expansion (only if requested).** Market compositions
   and Research strategy grids are each executable now, but an arbitrary
   Market graph is **not automatically** a new live/replay strategy definition.
   That requires an explicit typed adapter/strategy contract and new safety
   criteria; no generic DSL, second finance owner or implicit research→trade
   route is authorized by #518's current tests.

**No release claim:** Issue #518 remains OPEN. PR #529 remains Draft. No
deployment, Windows installation, activation, RiskBook/TradeService change,
financial mutation or venue order occurred as part of these CODE tests.

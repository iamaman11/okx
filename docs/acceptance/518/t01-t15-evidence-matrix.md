# #518 — T01–T15 evidence matrix (CODE, not release acceptance)

**Evidence cutoff:** 21 November 2026 (Europe/Minsk). Canonical repository:
[`iamaman11/okx`](https://github.com/iamaman11/okx). Accepted native release baseline
[`630b8a4d1b29564c7c7391ed8c81dfd09e8d69f5`](https://github.com/iamaman11/okx/commit/630b8a4d1b29564c7c7391ed8c81dfd09e8d69f5)
has tree `4f533f877b82a4c254f70963bf6bcdb42b3cd315`, identical to the
accepted PR #530 test head. Subsequent [MCP fix PR #532](https://github.com/iamaman11/okx/pull/532)
merged into `main` as [`d5106afc775950718a05d2dce0ce9bc0844dc7a1`](https://github.com/iamaman11/okx/commit/d5106afc775950718a05d2dce0ce9bc0844dc7a1)
with tree `d954e5e063fa959ea68b68e765a9a9d8195e9a74`, identical to its fully tested head. [PR #529](https://github.com/iamaman11/okx/pull/529)
and [PR #530](https://github.com/iamaman11/okx/pull/530) are **MERGED**.
[Full exact-tree CI #4739](https://github.com/iamaman11/okx/actions/runs/44912048553)
and [post-merge CI #4740](https://github.com/iamaman11/okx/actions/runs/44914879177)
both **SUCCESS**. Full native finance acceptance: **FULL_PASS, 61/61 contracts,
106 witnesses**. Measured 100-sample p95: Native Finance 0.5457 ms,
Research explain 0.6235 ms and MCP→Native Finance 2.3279 ms under synthetic
Research load, each below the unchanged 100 ms SLO.

**Native installed:** verified four-EXE artifact `14710029752`, exact source
tree, all terminal Windows CONTROL #12 install operations PASS, Agent running
at PID 9072, actual versioned Research Worker child PID 2008, all four EXE
SHA256 values matching the verified manifest; release parity `MATCHED`.
[Full installation evidence](https://github.com/iamaman11/okx/issues/518#issuecomment-7064348654).

**Public deployment observed:** Cloudflare `diagnostics.runtime` now reports
`release_source_tree=d954e5e063fa959ea68b68e765a9a9d8195e9a74`,
transport PASS/connected/fresh, Agent PID 9072 with the installed SHA256.
Live read-only `market.capabilities` request
`req_mcp_b24cada9-4a61-495f-ba1f-6d5ce4384ca3` **completed**, `quality=complete`,
16 typed operators and 21 composition primitives including the versioned pure
`rolling_range` descriptor. The previous `RESPONSE_TOO_LARGE` (13,775 vs 12,288
bytes) is resolved in public MCP through the special 20,480-byte bounded
read-only catalog; normal results retain the 12,288-byte bound.
This verifies the public catalog, **not** every live composition/economics test.
No account is connected; no order was submitted.

## How to interpret a row

- `PASS` means the named **CODE** contract and negative controls are covered
  by source inspection and executed tests of the accepted source tree. It
  never means the complete public Cloudflare, installed, PHYSICAL, REAL or
  final T01–T15 acceptance or trading permission.
- `BLOCKED` means a required observed proof is absent; this is not a waiver.
- `NOT_RUN` means that **the specific named** campaign has not run. Inherited
  evidence from an older tree does not substitute for exact-tree proof.
- External capability `unavailable` is not automatically a test failure:
  several T06/T09 criteria **require** explicit unknown/unsupported outcomes
  instead of fabricated data or financial conclusions.

The **single integrated final T01–T15 owner/public/source-level review remains
NOT_RUN** even though its CODE and Native Windows prerequisites have substantially
passed. The table records narrower evidence and remaining release qualification.

| Criterion | CODE | Direct executable witness / public boundary | Remaining qualification |
|---|---|---|---|
| **T01 — catalog** | **PASS** | `okx-research-worker::analytics::tests::market_capabilities_discloses_supported_operator_names_and_units`; MCP `execution_tools.test.ts`: 16 distinct IDs, valid/invalid ID, strict 14 accepted and 15/16 rejected, parity with Native descriptors. | Production Cloudflare tree `d954e5e` now serves the full public 16+21 descriptor catalog read-only; `rolling_range` typed metadata verified. Public catalog alone is not historical-source availability or complete T01 negative-route acceptance. |
| **T02 — units and shape** | **PASS** | `okx-kernel::query_plan::t02_scalar_window_and_contract_currency_mix_fail_before_sources`, `t02_t03_t13_serialized_checked_plan_roundtrips_only_after_recompilation`; `okx-research::economics::t02_t09_linear_contract_conversion_is_typed_and_shorts_reverse_gross`; Worker rejects OI/funding composition on spot before reads. | Contract conversion is tied to an explicitly supplied/current instrument specification, not a verified historical spec schedule. |
| **T03 — step identity** | **PASS** | `okx-kernel::query_plan::t03_checked_composition_allows_branching_and_repeated_ops_but_rejects_bad_refs`; Worker `t02_t03_public_row_uses_checked_operator_and_parameter_binding`; no second graph interpreter or source owner. | A different operator ID/parameter means a different typed plan; no arbitrary unchecked JSON execution. |
| **T04 — grid and quality** | **PASS** | `okx-kernel::time_series::t04_exact_five_bar_grid_distinguishes_missing_duplicate_and_right_boundary`; Worker strict candle/OI coverage; real Agent→Worker `t06_real_worker_cross_instrument_oi_alignment_is_strict_and_isolated`: 100/100 versus 99/100 at +1 ms, no nearest/forward fill. | Source-specific actual historical coverage depends on the venue/archive. |
| **T05 — numeric semantics** | **PASS** | `okx-kernel::query_plan::t05_returns_series_can_feed_a_ratio_window_without_changing_scalar_returns` and `t05_t10_sliding_mean_agrees_with_independent_window_oracle_and_propagates_gaps`; Worker `t05_composition_reuses_closed_candles_and_projects_a_distinct_ratio`. | f64 statistics have specified tolerances; exact settlement cashflows use Decimal, not f64. |
| **T06 — time joins / Pearson** | **PASS** | Kernel `t06_sparse_event_grid_is_exact_and_does_not_invent_publication_time`, `t02_t06_rolling_pearson_requires_thirty_exact_pairs_and_preserves_coefficient_type`; Worker `t06_pearson_needs_thirty_known_aligned_pairs_not_future_or_shifted_data`; real child IPC causality + two-instrument alignment tests. | **Data capability unavailable:** historical publication timestamps of OI/funding are unverified, current ticker is not a historical spread; decision signals relying on unknown publication time remain `null`. |
| **T07 — event / label** | **PASS** | `okx-kernel::query_plan::t07_outcome_cannot_be_a_feature_or_event_condition`, prefix-invariance / historical `evaluate_at` checks; Worker `t07_event_graph_exposes_boolean_metrics_but_not_unconfirmed_future_outcomes`; Agent child `t07_t08_real_worker_intraday_event_and_holdout_stay_separate` on 1,440 bars. | Public `market.screen` projects the **last closed bar**; its unconfirmed future label is `null`. Historical `evaluate_at` is a pure Rust operation, not a live trading signal or a public batch-label export. |
| **T08 — strategy / OOS** | **PASS** | `okx-research::strategy_registry::breakout_grid_sixty_four_train_only_candidates_and_oos_isolation`; Worker `t08_public_breakout_grid_persists_typed_selection_and_rejects_65_before_sources`; Agent child 1,440-bar Grid: identical 1,008-bar train, changed 432-bar holdout, stable selected variant and candidate train hashes. | `close_breakout_v1` is a counterfactual Research definition; do not infer live strategy activation, authority or profitability. |
| **T09 — economics** | **PASS** | `okx-research::economics::t09_three_linear_contracts_exact_oracle`; `economic_audit` replays fills/fees/funding/equity, cross-currency cashflow buckets remain separate; Agent child `t06_t09_real_worker_causality_and_economics_are_isolated`: +3 USDT and +2 USD, no FX/joint risk. | **Data/model capability unavailable:** `historical_observed` refuses with `HISTORICAL_RULES_AND_FEES_UNAVAILABLE`; no historical fees, rule schedule, dated FX portfolio valuation, venue mark or option model is implied. |
| **T10 — resource limits** | **BLOCKED** | Exact-tree [full CI #4739](https://github.com/iamaman11/okx/actions/runs/44912048553) SUCCESS, 100-sample p95 Native Finance 0.5457 ms / Research explain 0.6235 ms / MCP Finance 2.3279 ms during synthetic load. Direct bounds cover 1M/1M+1 bars, 8/9 instruments, 64/65 variants, graph cells/depth, shared 4,096 MiB *estimated* permit, two heavy slots; child IPC rejects oversize requests before source access. Synthetic-source IPC counts and 20 ms sampled process-tree RSS/I/O are measured separately from build time. | Still need source-specific venue HTTP/storage-page I/O measurement, genuine whole-installation resource ceiling and peak/limit on fixed hardware, and cancellation/load evidence for the exact production source. Sampled RSS may miss peaks; summed RSS double-counts shared pages and is not a hard limit. Passing native Finance p95 alone cannot close all T10. |
| **T11 — effects / rights** | **PASS** | `scripts/check_architecture.py` isolates Worker from financial dependencies; worker capabilities declare no venue POST/finance/governance writes; IPC child tests cover research metadata, no-effect economics, read-only diagnostics and cancellation under saturated command lanes. | This is CODE/process-fixture evidence; no right to execute a real TradeService command is transferred by an artifact or `RecordDecision`. |
| **T12 — financial invariants** | **PASS** | Fresh [full Windows Native/Finance #4739](https://github.com/iamaman11/okx/actions/runs/44912048553) exact-tree **FULL_PASS 61/61 contracts / 106 witnesses**; Engine under Research load, native p95 0.5457 ms and MCP-to-Native p95 2.3279 ms, 100 samples each. Post-merge #4740 SUCCESS on identical tree; no stale #527 result reused for the changed shared Kernel/Protocol. | This is synthetic/counterfactual acceptance, not a live exchange POST or authorization to trade. Continue to require independent TradeService approvals, idempotency and UNKNOWN acknowledgement controls before any financial operation. |
| **T13 — repeatability** | **PASS** | Worker `t13_composed_market_route_persists_input_evidence_and_replays_same_job`, `t13_export_rechecks_all_replay_dataset_pages_and_exact_content_hash`, source-page reconstruction and bounded independent reexecution; Agent child immutable cursor paging, changed OOS dataset hash, old typed result reads. | Unverified publication time is never upgraded by a matching hash; large/unsupported replay explicitly says `not_verified`. |
| **T14 — lifecycle** | **PASS** | Worker `t14_market_retry_and_cursor_never_publish_cancelled_or_interrupted_partial_result`, `t14_partial_artifact_is_never_advertised_as_a_completed_result`; child `ipc_process` lost-ACK/restart/conflicting-key/real-process controls and Agent cancel/reopen tests. | Interrupted work is explicitly terminal/diagnosable, never silently published complete. Installation handoff is a separate release acceptance. |
| **T15 — extensibility** | **PASS** | [`t15-range-extension.md`](t15-range-extension.md): new pure versioned `rolling_range`, independent oracle, checked plan, Worker descriptor, MCP allowlist and two previously unnamed user graphs through the same `market.screen` route. | Public MCP allowlist and typed versioned `rolling_range` descriptor now exposed by the deployed Worker. A fresh live arbitrary-graph `market.screen` job is not covered by this catalog-only check; no arbitrary live strategy execution or TradeService dispatch is implied. |

## Actual remaining work, in the necessary order

1. **T10 real resource and source-I/O proof.** Run a bounded, instrumented
   acceptance on fixed identified hardware/OS with exact installed binary
   hashes. Separate compilation, child startup, wall-clock, actual source and
   storage-page reads, whole-process measured peak/limit and cancellation.
   Current synthetic SLO/RSS and code limits are useful but incomplete.
2. **Final T01–T15 integrated review.** Assemble input/output/reason, effect
   counts, source/economic assumptions and test logs on one frozen tree;
   classify each criterion PASS/FAIL/BLOCKED/NOT_RUN at CODE, INSTALLED,
   PHYSICAL and REAL levels separately. Mark genuinely unavailable external
   data as unavailable, not as an invented historical fact.
3. **Separate product expansion (only if requested).** Market compositions
   and Research strategy grids are each executable now, but an arbitrary
   Market graph is **not automatically** a new live/replay strategy definition.
   That requires an explicit typed adapter/strategy contract and new safety
   criteria; no generic DSL, second finance owner or implicit research→trade
   route is authorized by #518's current tests.

**Actual release claim:** PRs #529/#530/#532 merged; the accepted native Windows
four-component version **is installed/running** and the latest Cloudflare MCP
catalog is **deployed and verified read-only**. Final T01–T15 integrated
acceptance is **not yet certified**, and #518 remains OPEN. No account enrollment, RiskBook/TradeService mutation,
financial execution or venue order occurred as part of this acceptance.

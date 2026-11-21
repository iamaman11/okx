# #518: universal strategy composition — remaining gaps (2026-11-21)

Status: **design/acceptance backlog, not implemented or accepted by this document**.

## Verified current boundary

- `okx-kernel/src/query_plan.rs` implements **21** finite, typed causal
  `SeriesOp` variants: returns, rolling mean/min/max/range/Pearson/Z-score/
  standard deviation, addition/subtraction/division, AND/OR, lag,
  threshold/series comparisons, rising/falling edges and explicitly
  non-causal `forward_return_label` for outcome scoring only.
- `okx-kernel/src/operators.rs` lists **16** analytic `Operator` variants,
  including EMA/ATR/RSI and position/cost operators. **Listing an operator
  is not proof that it is available as a composition step.**
- `okx-research/src/strategy_registry.rs::replay` still calls
  `StrategyKernel::from_definition`. That kernel accepts a finite set of
  registered strategy IDs. Therefore an arbitrary valid `CompositionRequest`
  **cannot yet become a trade-generating replay strategy without new code**.

## P0 — universal graph-to-backtest bridge (highest practical value)

Define a typed strategy spec containing a validated causal signal graph,
explicit result step(s), market/timeframe/dataset identity, signal-to-target
position mapping, and the existing immutable costs and execution model.

- Compile the graph once; evaluate causally on **closed bars only** with
  bounded incremental state/cost, no O(N²) historical-prefix replay.
- Map valid signals to deterministic target exposure (flat/long/short),
  retaining unknown/missing values as **not evaluated**, never zero or a
  trade instruction; permit an explicit conservative flat policy only when
  it is recorded in the strategy spec.
- Feed the result into **one** existing money/replay accounting path,
  including next-bar-open execution, fees, funding, slippage and cancellation.
- Research produces an inspected recommendation/evidence artifact. No graph
  can directly bypass Native risk policy or send an execution mutation.
- Accept new graph-shaped strategies without recompiling any Rust or changing
  the registered `StrategyKernel` enum.

Negative acceptance: future-bar labels cannot influence decisions; changing
future prices does not change prior decisions/trades; missing inputs cannot
create implicit trades; fee/funding/cash identities match the existing native
replay oracle; resource budgets and cancellation are bounded.

## P1 — small, reusable algebra and state primitives

Add only with typed unit/shape and missing-value semantics:

1. `multiply_series`, `min_series`, `max_series`, `abs_series`, `negate_series`,
   and safe `log`/`sqrt` domain checks (nonfinite/missing propagates).
2. `rolling_sum`, `rolling_count`, `rolling_variance`, `rolling_covariance`,
   with explicit warmup and minimum valid observations.
3. `not`, `where` (three typed inputs), and explicit `is_valid`/`coalesce`
   policies for sparse series. `where` requires expanding the present
   `SeriesStep` single-input/`other` schema without an untyped expression VM.
4. Reusable finite-position state transitions: target exposure, hysteresis,
   cooldown and exit-on-unknown as explicit typed steps.

## P2 — robust statistics and market-specific measures

- Rolling quantile/median/MAD, winsorization with declared outlier policy,
  exponentially weighted variance, volatility regime and normalized ranks.
- VWAP and volume-weighted statistics; ATR/RSI/EMA as composition sources
  or steps (already known as scalar analytic operators, not all as `SeriesOp`).
- Cross-asset beta/correlation with strict timestamp/candle alignment and
  missing-source diagnostics; funding/open-interest regime normalization.

## P3 — multivariate and portfolio research

Finite covariance/correlation matrices, factor residuals, exposure and
turnover, tail-risk measures (VaR/expected shortfall), and multiple-instrument
target weights, with explicit capital/venue/mechanics units. These extend
Research capabilities, **not** financial authority in Cloudflare.

## Rules for adding any primitive

Pure deterministic implementation; typed inputs/outputs and units; no future
data read except separately labelled outcomes; missing/partial provenance;
O(N) or stated bounded complexity; cancellation and resource accounting;
replay/streaming equivalence; fuzz/property and boundary tests; published
capabilities and Cloudflare contracts stay synchronized. Pure experiments
composed from already supported primitives require **no build or release**.

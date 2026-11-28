# Adding a typed Research operation

This guide implements [#537](https://github.com/iamaman11/okx/issues/537) and [#684](https://github.com/iamaman11/okx/issues/684). It is not a second catalog or roadmap.

Research owns pure strategy mathematics. Engine owns live trading decisions, the financial SQLite writer and risk. CONTROL owns installation and process lifecycle. Edge owns authentication and strict request projection. An operation must not transfer execution or financial authority between owners.

## One existing path

1. Declare the versioned wire ID in `okx-research-protocol/src/strategy_contract.rs::SeriesOp` and the finite `SeriesOp::ALL` list.
2. Extend the **existing** `okx-strategy-core/src/query_plan.rs::series_operation_contract` with input/output units, parameter rules, warmup, null behavior, cost, causal status and supported graph routes. `CheckedComposition::compile` checks the actual upstream units and dependency order **before** source I/O.
3. Implement one pure numerical rule using the existing `query_plan` batch and `stream` incremental evaluators. Share the numerical primitive when the same formula already exists. Never add another evaluator, registry, executor or job store.
4. Extend the strict MCP schema in `cloudflare/okx-mcp/src/execution_tools.ts`. The Native contract remains the admission authority; Worker validation is a bounded, early rejection, not a mathematical executor.
5. Verify Rust descriptor/catalog parity, independent mathematical oracles, batch/stream agreement, missing-data behavior, future-tail invariance, MCP positive/negative validation and exact refusal reason.

## Examples

`scale_series(input=stddev, factor=2)` uses a **dimensionless** finite factor and preserves the input unit. `add_series(mean, twice_stddev)` accepts only matching numeric units; `Price + Ratio` fails compilation. `ema_series(close, period=9)` and `ema_series(close, period=21)` are separate stateful nodes. EMA seeds from the first closed value, withholds output until the full period and restarts warmup on a missing sample; its recurrence is shared with the registered Research EMA kernel.

The contract version of `add_series` advances to `2` for same-unit algebra, while all previous `Ratio + Ratio` graphs retain the same numeric semantics and the unchanged `okx.market.composition/v1` wire version. New operators start at contract version `1`.

## Composable logic (P0-C)

`not(Boolean)` inverts a known condition and keeps unknown unknown. `if_else(input=true_arm, other=false_arm, when=Boolean)` requires two branches with exactly the same causal unit; a missing *unused* arm does not invalidate the selected result. `rolling_min(Boolean, period=N)` already expresses N consecutive true closed bars, so it is not a separate operator. Its strict complete window returns unknown until warmup ends and whenever any input in the window is missing. `rolling_max(Boolean,N)` similarly expresses any true observation in a fully known window. Their wider Boolean contracts are version `2`.

`rising_edge(Boolean)` converts a level into an event; `cooldown(Event, period=N)` permits events at least N observed closed bars apart. An unknown event remains unknown and resets the gate. The original `and`/`or` keep their v1 strict-null semantics (one unknown operand yields unknown); changing this truth table would require an explicitly versioned wire contract. Timestamp monotonicity alone does not prove that every expected time slot was observed: the graph never invents missing bars.

## One diagnostic chain

Keep the existing `request_id → Research invocation → job_id → source/graph step → persisted result` identities. Preserve `reason_code`, `first_unproven_fact`, `quality`, `next_action`, owner and provenance where observed. A read-only explanation must not fetch market data, start a process or write financial state. Missing retained evidence stays `UNKNOWN` / `NOT_EVALUATED`, never a fabricated historical success.

Readiness is reported independently as **CODE**, **INSTALLED**, **PHYSICAL** and **REAL**. Green unit tests never imply an accepted Windows release or a supported historical source.

# Typed operations: one contract spine, multiple execution owners

This is the implementation-level invariant for extending typed calculations in
OKX, **not** a new execution engine, wire protocol, state store or independent
product specification. The source of truth is the canonical `main` tree and
its tests. The active acceptance cursor is [Level C #667](https://github.com/iamaman11/okx/issues/667),
within [#537](https://github.com/iamaman11/okx/issues/537); financial authority
remains governed by [the Engine contract](ARCHITECTURE_CONTRACT_V1.md).

## Existing composable inventory (do not overstate it)

| Family | Code owner | Current composition boundary |
| --- | --- | --- |
| 21 `SeriesOp` variants | `okx-research-protocol/src/strategy_contract.rs` IDs; `okx-strategy-core/src/query_plan.rs` contracts and checked compiler | Finite versioned `CompositionRequest`; all 21 are representable in a Market graph |
| 20 causal `SeriesOp` variants | `okx-strategy-core/src/stream.rs` | Bounded incremental `CausalGraphStream`; `forward_return_label` is an outcome **only**, never a signal input |
| Seven typed series source ports | `okx-strategy-core::query_plan::SeriesSource` and `SeriesSourceContract`: `open`, `high`, `low`, `close`, `volume`, `open_interest`, `funding_rate` | One finite versioned source/unit/time/scope/alignment catalog consumed by `CheckedComposition`, `QueryPlan`, Market discovery and Research graph discovery; existence is **not** proof of venue publication time |
| Graph research backtest | `okx-research/src/graph_signal.rs` and `strategy_registry.rs`, served by `okx-research-service` | Only `close` source and supported causal operations; next-open replay through the existing Research accounting path |
| 16 analytic `Operator` variants | `okx-strategy-core/src/operators.rs`, `query_plan.rs` | Separately registered Market scalar analytics. **Not** 16 additional arbitrary `SeriesStep` nodes |
| Statistical/economic/portfolio routines | `okx-analysis`, `okx-research` | Reusable native functions and finite Research actions, not general graph opcodes |

Market/Research capabilities must describe the **actual executable subset**,
not the union of visible names. A compiled Market graph is not automatically
an admissible graph backtest. `historical_observed` and graph-wide Grid selection
must not be advertised without independent accepted evidence. A **single
predeclared graph** has a 70/30 train/holdout evaluation through the existing
Rust replay owner; this is a fixed hypothesis, **not** fitted parameters or
graph-wide Grid selection. The train dataset/hash never includes holdout bars,
and the holdout ledger starts afresh using only earlier closed bars as context.
Existing named Grid selection/OOS remains a distinct feature.

## Contract model

Every *pure composable* operation has one Rust-owned descriptor, from which
discovery metadata is serialized. Its effective type is the tuple:

```
ValueType = (shape, unit, instrument_scope, timeframe_and_range,
             observation_time, availability_time, missing_policy)
```

- **Shape:** scalar, aligned series, boolean series, event series, or
  outcome-only result. Currently the 21 `SeriesOp` outputs are series;
  unit (`Boolean`, `Event`, `OutcomeRatio`) distinguishes their meaning.
- **Unit:** use the existing closed `Unit` and `SeriesUnitRule` predicates.
  Prices, contract counts, native volumes, ratios and quote currency are
  different dimensions; never silently coerce one into another.
- **Scope:** instrument/venue and quote/settlement currency matter whenever
  quantities are combined. Two price series with equal `Unit` but differing
  instruments or quotes are not automatically compatible. The native
  `BoundSeriesSourcePort` now binds the exact instrument, base asset, quote
  currency, product and closed range **per input port and per instrument**.
  Multi-instrument Market queries retain independent bindings; this is not
  permission for a cross-instrument or cross-currency join.
- **Time:** declare closed-bar grid and event timestamps separately from
  *when the value became knowable*. Closed candle alignment is not proof of
  historical availability for OI or funding. Unknown publication time fails
  closed for causal strategies; future outcomes may only be used for scoring.
- **Source admission:** the seven existing source names have one closed Rust
  `SeriesSource` enum and versioned `SeriesSourceContract`. Unit, source owner,
  denomination basis, alignment, swap-only scope, read-only access and availability requirements
  are shared by the compiler and discovery. The Market `input_sources` and
  `source_units` v1 fields remain unchanged and derive from this catalog;
  `source_contracts` exposes its typed detail. The Research graph source list
  derives the current `close`-only subset from the same contracts. A metadata
  flag cannot enable a missing physical input port in the stream evaluator.
  The bound port is **derived**, not a new persisted plan or separate runtime.
  It distinguishes quote-per-base price, unresolved native volume, contract
  count requiring exchange-reference evidence and dimensionless funding.
  Quote currency parsed from an instrument is *not* proof of a swap's actual
  settlement currency. The current Research graph also preflights the same
  `close` binding before any job admission or source request.
- **Missing data:** unknown is not zero, false or permission to trade. Warmup,
  gaps, zero denominators, incomplete windows and future-horizon outcomes
  retain specific semantics. Any explicit `flat` signal policy is part of the
  versioned `GraphSignalSpec`.

Each descriptor also declares immutable `id`/contract version, required
inputs and optional parameters with limits, causality, computation cost and
resource ceiling, effect/permission class, implementation owner and
independent verification identity. **Semantically significant properties must
be closed Rust enums or validated newtypes**, not free-form string policies.
Stable string labels remain acceptable only as their serialized public names
or diagnostics text, never as a second admission authority.

There is one descriptor per operation **within its existing owner**, not one
giant global registry that imports every crate. `SeriesOp` contracts feed
Market and Research capabilities. Scalar analytics keep their separate finite
catalog until a real typed graph implementation exists. Public source requests,
Research jobs, CONTROL lifecycle commands and financial Engine actions are
typed *actions* with different effect classes; they are **not** arbitrary
nodes in the pure `SeriesOp` expression graph.

## Admission and execution invariant

1. Decode the finite, versioned, bounded request into the existing protocol
   DTO. Reject unknown fields and unsupported operation IDs/versions.
2. Validate IDs and dependencies (no unknown/forward references, cycles,
   excessive depth or duplicate projections).
3. Resolve the actual unit of each parent using the registered contract,
   check parameter ranges and the expected output. Check instrument and
   time-grid compatibility where supported; reject unsupported combinations.
4. Verify the declared source *capability* and causal availability. Source
   presence in a capability catalog is not a guarantee that historical data
   will be fetched successfully; actual coverage and content checks run at
   the existing source owner and can still return a typed failure.
5. Evaluate the whole-plan work/cell/memory budget and allowed effect class.
   Outcome values cannot flow back into causal strategy decisions.
6. Only the existing compiler constructs `CheckedComposition`/`QueryPlan`.
   Deserialized checked values are recompiled and cross-field bindings are
   rechecked; untrusted serialized annotations do not bypass validation.
7. Dispatch to the existing Rust executor. Market computes queries; Research
   owns hypothetical evaluation, job admission, cancellation, results and
   immutable artifacts. Live strategy decisions and all real trades stay in
   Engine under its own authorisation and risk gates.

Known-invalid plans fail **before** creating a job, allocating work or reading
the source. Runtime source failures may occur later and must keep their real
failed/partial state. Never invent a second universal interpreter, workflow
runtime, SQL writer, market source adapter or Research-to-Engine trade lane.

## One diagnostic vocabulary, existing owners of evidence

For every supported operation, an operator must be able to identify:

| Question | Existing or required evidence |
| --- | --- |
| Which operation? | Contract ID/version, named `step_id` and known graph path |
| Whose responsibility? | Existing Market, Research, Engine or CONTROL owner and stage |
| Which request? | Original `request_id`/Research invocation, `job_id` or scoped Engine `intent_id` as actually observed; these IDs are **not interchangeable** |
| Why did it stop? | Bounded typed `reason_code`, field path, expected/actual unit where safe; source vs preflight vs execution vs publication distinguished |
| Can it be repeated? | Existing idempotency and retry classification; no automatic retry of an uncertain financial mutation |
| What was proved? | Source generation/content hashes, checked plan, operation/code version, exact costs, budget, result and evidence IDs |
| What is unknown? | Missing request/ACK/history must remain `UNKNOWN` or `NOT_EVALUATED`, not invented successful or no-effect status |

Use the existing read-only [`diagnostics.explain`](OPERATION_DIAGNOSTICS.md)
and Research catalog, error projections and artifact store. Do not create a
second log database or store unbounded/raw source errors in diagnostics.
`operation_trace` is an observed **partial** causal summary, not an all-events
proof. Failed 0/N batches must remain failed; a mixed 1/N result is partial;
only a successfully published and verified result may be completed. The
published result's hashes prove artifact integrity within their verification
scope, not the full network or request history.

### Independent Research export proof

The existing native Research export returns separate, bounded verification
layers. `counts_verified` means the immutable trade/row/equity **page counts**
and SHA identities were inspected; it does not assert that the replay produced
the correct rows. `summary_trades_reconciled` additionally links a replay
summary's trade count to its declared, hash-checked trade pages. Experiment
source and funding provenance are reported by `sources_verified`; graph and
OOS policy references are structurally bound to immutable experiment inputs
even if a full replay cannot be repeated.

An export's **integrity `quality: complete`** requires all of:

1. `sources_verified: true` (dataset and applicable funding verification);
2. `summary_trades_reconciled: true` (cross-artifact count agreement);
3. `semantic_reexecution_verified: true` (independent bounded native Rust
   replay, including exact trades/equity, summary and economic audit).

For unsupported versions, missing legacy audit/inputs or work exceeding the
reexecution budget, the original result remains readable, but the export's
integrity is `partial` with the concrete reason in `reexecution.reason_code`.
A successfully published `Completed` job therefore does **not** by itself
claim an independently verified export. No export quality proves historical
venue fees, mark prices or the actual publication time of observations.

## Acceptance rule for one more building block

An operation is *available* only after the following changes land together:

1. Add its unique versioned ID, strict input/output contract and bounded
   parameters to the existing finite Rust catalog; specify unit, time,
   missing, causal, cost, effects and source requirements.
2. Implement it once in the responsible **native Rust owner**. Prefer shared
   batch/stream numerical kernels; do not copy calculation logic into the
   protocol, Worker, Edge or diagnostic adapter.
3. Test valid/invalid units, mixed instruments/timeframes, history gaps,
   no-future-leak, nonfinite values, resource overflow and cancellation.
   Where both batch and streaming are advertised, test equivalent results
   over the same accepted closed-bar prefix.
4. Verify deterministic reexecution, immutable source/version provenance and
   typed failure behavior, including failed/partial/cancelled outcomes.
5. Derive the public capability from that same catalog; add a test that the
   published supported set equals executable/admissible operations. A graph
   operator can be Market-supported but unavailable for Research.
6. Run component-scoped CI and dependency/owner boundary checks. Research-only
   work must not rebuild/restart Engine or acquire account/finance access.
   Distinguish passing CI from an accepted installed Windows component.

New *combinations of already supported operators* require no Rust change.
New *elementary functions* require exactly this one owner-bound contract,
implementation and tests. Do not add a new use-case-specific executor.

## Current acceptance gap

[#667](https://github.com/iamaman11/okx/issues/667) tracks completion of
Level C. The 21-operation metadata registry, incremental causal parity and
typed graph preflight, single-graph train/holdout OOS and bounded independent
cross-artifact export/replay consistency have landed. Source-port binding now
checks per-instrument product, quote currency, source denomination and closed
range at admission. No cross-instrument algebra or unverified currency
conversion is advertised. Remaining gates include independently proven
publication/settlement context for additional Research sources, installed
Research-only T10 and the
separate live all-429/cancellation acceptance tracked in
[#666](https://github.com/iamaman11/okx/issues/666).

Architecture ownership and operational diagnostic constraints remain in
[ARCHITECTURE.md](ARCHITECTURE.md),
[research-boundaries.md](research-boundaries.md), and
[OPERATION_DIAGNOSTICS.md](OPERATION_DIAGNOSTICS.md).

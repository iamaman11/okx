# Research and live execution

This is a source map, not another execution model. The authoritative boundaries
are Cargo dependencies, Rust visibility, the IPC enums, and the SQL owners.

## Ownership

| Responsibility | Production entrypoint / owner |
|---|---|
| Authenticated product command | `okx-agent::query::dispatch_with_governance` |
| Available or unavailable finance | `okx-agent::finance_dispatch::FinanceDispatch` |
| Existing finance startup and exact rejection cause | `okx-agent::finance_startup::select` |
| Live execution, protection, reconciliation | `okx-agent::trade_service::TradeService` |
| Financial persistence and RiskBook | Existing `UnifiedFinanceHandle`, one financial SQLite |
| Live closed candles | `okx-runtime::live_source::LiveCandleSource` |
| Public market HTTP and shared rate budget | `okx-runtime::market_bootstrap::MarketBootstrapper` |
| Research Service child, request correlation, activation | `okx-agent::research_client::ResearchClient`; installed binary `okx-research-service` |
| Framing and typed public-source requests | `okx-protocol::research_ipc` |
| Research command handler | Internal crate `okx-research-worker::analytics::AnalyticsService` |
| Research jobs, identity, cancellation, root lease | Research Service `analytics::jobs::Jobs` |
| Pure calculations | `okx-strategy-core`, `okx-analysis` and `okx-research` libraries |
| Verified component staging | Existing CONTROL `artifact::research_component` |

Agent has no production dependency on the Research Service implementation.
Research Service has no production dependency on Agent, execution, API credentials, or
financial storage. Its test-only HTTP fixtures do not authorize those edges.
`scripts/check_architecture.py` checks both direct and transitive dependencies.

Research requests receive public observations through the existing host source
owner. They cannot issue arbitrary HTTP, access an account, reserve money, or
place orders. The legacy live `strategy_turn` still synchronously invokes
`EvaluateSignal` on Research Service per closed-bar decision. The new autonomous
graph feed exists but has not yet replaced that live trading loop.

## Graph wire contract (#537)

`okx.graph/v1` uses one shared envelope through MCP, Agent and Research:

```json
{
  "schema": "okx.graph/v1",
  "steps": [{
    "id": "mean_close",
    "op": {"id": "rolling_mean", "version": 1},
    "inputs": [{"source": "close"}],
    "params": {"period": 20}
  }],
  "outputs": ["mean_close"]
}
```

An input is exactly one of `{ "source": "close" }` or
`{ "step": "previous_id" }`; later steps cannot be referenced. Inputs are
ordered; Research assigns their meaning according to the installed operator.
The positive operation version is `u32` and is independent of the envelope
schema version. Public validation checks only this structure and bounds, not
the list of operations or their parameters. Research resolves operations and
units, rejects unsupported versions with the step, ID and version, and runs
the same typed compiler as the legacy composition request. The previous
`okx.market.composition/v1` request remains a separate compatible route.

The obsolete `version`/`op_version`/`result_steps` graph envelope was an
unreleased Draft representation; it is not accepted as `okx.graph/v1`.
The public request ceiling remains 64 KiB and IPC framing remains 4 MiB.

## Strategy activation versus position targets

These are distinct commands with distinct owners:

1. A client submits `StrategyRequest::Create` to register an instance, then
   `StrategyRequest::Start` to enable it (or `Stop` to disable new strategy
   decisions). Engine persists the strategy's enabled state, version and risk
   configuration in the original financial owner. `Start` is **not** an order.
2. Agent sends a read-only `GraphFeed::Start` for a graph session; Research
   Service obtains closed public candles through Agent's typed source proxy and
   publishes `GraphSignalPublication` with a typed `GraphSignalIntent`.
3. The semantic signal is `Hold` (do nothing), `SetTarget(Flat)` (target no
   strategy exposure), `SetTarget(Long)` or `SetTarget(Short)` (target direction).
   It is **not** a trade command, order ID, budget grant or exchange POST.
4. Engine compares the requested target with **its own** strategy/instrument
   position, pending orders, account policy and risk. The pure
   `okx-execution::position_target::plan_strategy_target` calculates one
   reconciliation phase: no change, open, close existing references first, or
   await settlement. It never sends an order. Only the existing durable Engine
   decision, risk/admission and venue execution route may act on a plan.

The next staged boundary, `TradeService::preview_graph_publication`, now reads
one scoped strategy/lot/in-flight snapshot from the **original unified finance
SQL owner** and binds it to an exact Research session, graph hash, strategy
version and policy revision. The typed publication cursor is checked on a
clone: **preview never ACKs, stores, or executes** the proposal. Outstanding
`UNKNOWN` orders block a new opening and existing lots from unrelated
strategies/accounts do not count. A real Research child IPC publication is
exercised through this pure preview in tests, without venue POST.

This is a read-only Engine adapter, **not wired into the live trading loop**.
The legacy loop currently treats `SignalPosition::Flat`
like `Warmup` (no new entry), so it does not flatten existing lots for `Flat`.
This is a tracked behavior gap, not a claimed live fix. Before connecting graph
publications to trading, Engine also needs durable per-instance cursor/ACK,
recovery after restart, stale-generation protection and serialization of
pending opens/closes; missing Research must not be mistaken for `Flat` or
`Hold`. Manual `TradeRequest::Execute` remains a separate, explicit one-order
request through the same financial Engine.

## Identity and recovery

`RequestId` belongs to one IPC session. `IdempotencyKey` identifies a durable
command in the authenticated host's research scope. `JobId` identifies its
persisted job. Reconnection does not manufacture a new idempotency key.

The existing research catalog owns semantic request digests, scope binding,
terminal state, and cursor identity. An exclusive root lease precedes recovery.
Interrupted work is reported as interrupted; it is not promoted to success.
Cancellation is terminal even when a previously issued source response arrives.

`InvocationId` links the originating product request to the IPC request and job;
it is not the durable idempotency key. The catalog records the creator's actual
Worker executable SHA-256 and available build-time source identity at admission.
Reopening under another executable preserves that identity. Result manifests
record the executable that computed the result. Legacy records retain unknown
provenance rather than inheriting the currently running Worker.

Financial startup retains a typed cause and returns the original safe rejection
code. Missing, corrupt, busy and foreign storage are not interchangeable. Research
does not require successful finance startup or an initial HTTP instrument fetch.
The existing public source owner initializes the reference only when a request
needs it; until then reference-dependent requests fail closed. Capabilities and
stored-job access do not perform that fetch.

## Installation versus activation

`HostControlOperation::DeployResearch` dispatches through `okx-host-control::runtime`
to `artifact::research_component::deploy_research`. CONTROL verifies the source,
component origins, compatibility and installed non-Research components, stages a
verified content-addressed worker and publishes
`installed-research.json`. This does not replace the running child. Agent detects
the candidate, drains admissions, waits for old jobs, commands, and public-source
handlers, then closes the old process before starting a verified replacement.
Active and installed versions are separate observations. Invalid staging never
authorizes a new executable or restarts the financial Engine.

`deployment_status` reads only a cached projection. `diagnostic_job` does not
start a child, refresh market sources, enqueue research, or recover finance.
A diagnostic timeout does not terminate the worker.

Account diagnostics read NAV completeness, raw coverage, source time and checked
time from one scoped financial snapshot, with a 250ms read deadline. They report
the current observed state, not an inferred historical admission cause, and
never refresh sources or grant a send. Coverage includes all five fill-history
partitions, bills and the account snapshot.

The financial collector retains verified earlier packages after a later
failure. Its property report distinguishes PASS, observed FAIL, NOT_PROVEN,
NOT_RUN and MISSING; the overall gate still fails on any incomplete execution.
Concurrent fixture tests report an early Execute reply immediately rather than
waiting for a phase the operation can no longer reach.
Finite fill/Close transcripts pause their timer producer only while explicitly
delivering history. Shutdown cancels a producer waiting for that test-only gate
before the gate is released. Final economic assertions inspect stored COMPLETE
coverage and ledger amounts; they do not assume a paused source stays fresh.
Admission freshness remains independently enforced and negatively tested.

## Direct checks after building

```powershell
cargo build --locked -p okx-research-worker --bin okx-research-service
$env:OKX_RESEARCH_WORKER_TEST_EXE = (Resolve-Path target/debug/okx-research-service.exe).Path
cargo test --locked -p okx-research-worker
cargo test --locked -p okx-agent --lib research_dispatch_tests::
cargo test --locked -p okx-agent --lib research_client::
cargo test --locked -p okx-agent --lib query::public_dispatch_tests::
cargo test --locked -p okx-agent --lib cloudflare_transport::session_tests::
cargo test --locked -p okx-agent --lib finance_startup::
cargo test --locked -p okx-runtime --lib public::reference_refresh::tests::
```

The dedicated Engine-under-Research-load check runs in its own test process so
Windows job accounting includes its descendants rather than unrelated tests.
Compilation, process startup, execution, and resource evidence are reported
separately. Measured resources do not by themselves establish a resource budget.
The first fully green four-component candidate was `dacab8c5` in
[CI #44222225744](https://github.com/iamaman11/okx/actions/runs/44222225744).
Its direct financial budget was verified, while the Research process-tree
resource budget was explicitly `NOT_PROVEN`. The latter is not a shipping resource
guarantee merely because the enclosing CI job succeeded.

In the full CI, Linux runs the Worker library, real IPC children and doctests
in the dedicated Research step; the portable workspace step excludes that
package. Windows runs the Worker library once, except for the K9 workload which
runs once in its dedicated evidence step. The Native regression excludes only
exact property witnesses already verified by a FULL_PASS from this same clean
SHA/tree and the current Rust manifest. Missing or mismatched proof blocks the
exclusion. Without a property run, no property witnesses are excluded. Financial
property acceptance and all remaining regressions are mandatory. Compare local
and CI timings for the same command and include its build time; a financial
execution time is not the duration of a complete Windows job.

To add an experiment with existing rules, submit another validated
`ResearchRequest` specification. To add a calculation, extend its existing pure
operator/replay registry and its direct tests, then connect it through the worker
planner. Financial state and CONTROL do not belong to that calculation change.

Research-only CI needs a previously successful, accepted and **merged** complete
base, an unexpired exact GitHub bundle and its explicit
`.github/research-release-base.json` selection. The first full-build PR has no
such prior merged base, so its skipped `research-only-windows` step does not
prove the next independent update. The separate Worker Cargo build, scope
negative controls, real child handoff and CONTROL staging checks provide CODE
evidence for the implemented route; actual use of a subsequent Worker release
is a separate acceptance event. No artificial second implementation is needed.

CODE acceptance remains governed by #512: C0–C13, U01–U05 and the exact-SHA
required gates are accepted only with their recorded evidence. Temporary fixture
process tests are not evidence of installation, real exchange connectivity, or
live trading. The [operation map](acceptance/512/operation-map.md) links the
current supported families to their owners and direct tests.

# Research and live execution

This is a source map, not another execution model. The authoritative boundaries
are Cargo dependencies, Rust visibility, the IPC enums, and the SQL owners.

## Ownership

| Responsibility | Production entrypoint / owner |
|---|---|
| Authenticated product command | `okx-trading-engine::query::dispatch_with_governance` |
| Available or unavailable finance | `okx-trading-engine::finance_dispatch::FinanceDispatch` |
| Existing finance startup and exact rejection cause | `okx-trading-engine::finance_startup::select` |
| Live execution, protection, reconciliation | `okx-trading-engine::trade_service::TradeService` |
| Financial persistence and RiskBook | Existing `UnifiedFinanceHandle`, one financial SQLite |
| Live closed candles | `okx-market-runtime::live_source::LiveCandleSource` |
| Public market HTTP and shared rate budget | `okx-market-runtime::market_bootstrap::MarketBootstrapper` |
| Research child, request correlation, activation | `okx-trading-engine::research_client::ResearchClient` |
| Framing and typed public-source requests | `okx-ipc-contracts::research_ipc` |
| Research command handler | `okx-research-service::analytics::AnalyticsService` |
| Research jobs, identity, cancellation, root lease | Worker `analytics::jobs::Jobs` |
| Pure calculations | Existing `okx-analysis` and `okx-research` libraries |
| Verified component staging | Existing CONTROL `artifact::research_component` |

Agent has no production dependency on the research implementation or worker.
Worker has no production dependency on Agent, execution, API credentials, or
financial storage. Its test-only HTTP fixtures do not authorize those edges.
`scripts/check_architecture.py` checks both direct and transitive dependencies.

Research requests currently receive public observations through the Engine's
host source owner (`ResearchClient::installed` delegates to
`MarketBootstrapper::research_source`). They cannot issue arbitrary HTTP,
access an account, reserve money, or place orders. **This is an existing
runtime dependency, not the desired independent Research boundary.**

The current live strategy turn in
`okx-trading-engine/src/trade_service/strategies.rs` loads closed prices from
the Engine's live source and awaits `ResearchClient::evaluate_signal` before
claiming a decision in financial SQLite. Thus the Engine does invoke Research
for **new strategy decisions**. This must not be mistaken for an independent
trading strategy runtime, even though Research is a separate executable and
is not a production Cargo dependency of the Engine. The existing financial
writer, RiskBook, reconciliation and protection remain Engine-owned.

The target split is: Research owns public data, numerical strategy evaluation
and a durable proposed-signal outbox; Engine owns the authenticated, versioned
signal inbox, policy/admission, financial SQLite and every venue mutation.
`EvaluateSignal` must leave the synchronous trading turn only after a tested
replacement exists. A proposed signal never constitutes risk authorization or
a one-time venue-send right. Research failure must block unjustified new
strategy risk without blocking manual trade commands or ongoing protection.

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

`HostControlOperation::DeployResearch` dispatches through `okx-host-controller::runtime`
to `artifact::research_component::deploy_research`. CONTROL verifies the source,
component origins, compatibility and installed non-Research components, stages a
verified content-addressed worker and publishes
`installed-research.json`. This does not replace the running child. Agent detects
the candidate, drains admissions, waits for old jobs, commands, and public-source
handlers, then closes the old process before starting a verified replacement.
Active and installed versions are separate observations. Invalid staging never
authorizes a new executable or restarts the financial Engine.
If CONTROL changes or damages the candidate after the old child has entered
Drain, the Engine must not return that permanently drained connection as healthy.
It revalidates the previous content-addressed Research image and restarts that
previous child; if even that image is invalid, Research fails closed while the
financial Engine continues. An aborted handoff can change the Research PID.

Call `diagnostics.explain` with `view: "research"` and `reference: "deployment"` to read
the Engine PID and its cached deployment observation. The Research PID, active
SHA-256 and handshake time describe the child that passed the IPC handshake.
`observed_at_ms` is the Engine clock at which this cached observation was read,
not the time the child was activated. Both timestamps are needed to prove that
before/after readbacks bracket a CONTROL change.
The installed SHA describes the candidate observed during the last connection
check. It can differ from the active SHA while the old child drains. To drive
an on-demand activation, call `research.capabilities`, then read the deployment
observation again. Check the returned active SHA and PID after both update and
rollback. When routed through the updated MCP Worker, the same response includes
the Edge version and release tree alongside the correlated Engine response.
Use CONTROL for executable and Launcher verification on Windows.

Supplying a job `reference` to the same view reads that job. Neither diagnostic
starts a child, refreshes sources, enqueues research, or recovers finance.

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
cargo build --locked -p okx-research-service --bin okx-research-service
$env:OKX_RESEARCH_SERVICE_TEST_EXE = (Resolve-Path target/debug/okx-research-service.exe).Path
cargo test --locked -p okx-research-service
cargo test --locked -p okx-trading-engine --lib research_dispatch_tests::
cargo test --locked -p okx-trading-engine --lib research_client::
cargo test --locked -p okx-trading-engine --lib query::public_dispatch_tests::
cargo test --locked -p okx-trading-engine --lib cloudflare_transport::session_tests::
cargo test --locked -p okx-trading-engine --lib finance_startup::
cargo test --locked -p okx-market-runtime --lib public::reference_refresh::tests::
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
runs once in its dedicated evidence step. The Native regression skips only
exact property witnesses extracted from the current Rust source manifest.
The parallel financial collector must execute FULL_PASS on the same clean
SHA/tree and prove that its observed roster matches that manifest. Missing,
failed or mismatched collector evidence makes the protected Windows gate RED;
the source roster alone never grants financial acceptance. All remaining
regressions are mandatory. Compare local
and CI timings for the same command and include its build time; a financial
execution time is not the duration of a complete Windows job.

To add an experiment with existing rules, submit another validated
`ResearchRequest` specification. To add a calculation, extend its existing pure
operator/replay registry and its direct tests, then connect it through the worker
planner. Financial state and CONTROL do not belong to that calculation change.

Research-only Cargo tests run without rebuilding the financial Engine even if
the accepted binary base is unavailable. Deployable **Research-only packaging**
still needs a previously successful, accepted and **merged** complete base,
an unexpired exact GitHub bundle and its explicit
`.github/research-release-base.json` selection; otherwise Ready fails closed.
The first full-build PR has no
such prior merged base, so its skipped `research-service-tests-windows` step does not
prove the next independent update. The separate Worker Cargo build, scope
negative controls, real child handoff and CONTROL staging checks provide CODE
evidence for the implemented route; actual use of a subsequent Worker release
is a separate acceptance event. No artificial second implementation is needed.

CODE acceptance remains governed by #512: C0–C13, U01–U05 and the exact-SHA
required gates are accepted only with their recorded evidence. Temporary fixture
process tests are not evidence of installation, real exchange connectivity, or
live trading. The [operation map](acceptance/512/operation-map.md) links the
current supported families to their owners and direct tests.

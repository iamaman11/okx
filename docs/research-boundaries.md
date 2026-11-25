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
| Legacy Engine-hosted Research child, request correlation, activation | `okx-trading-engine::research_client::ResearchClient` |
| Framing and typed public-source requests | `okx-ipc-contracts::research_ipc` |
| Research command handler | `okx-research-service::analytics::AnalyticsService` |
| Research jobs, identity, cancellation, root lease | Worker `analytics::jobs::Jobs` |
| Pure calculations | Existing `okx-analysis` and `okx-research` libraries |
| Verified component staging | CONTROL `artifact::research_component` |
| Optional standalone Research process, Windows Job and desired state | CONTROL `research_supervisor::ResearchSupervisor` |
| Standalone Research Cloudflare credential | Research `edge_credentials`, separate Windows Credential Manager target |

## Independent Edge lane (prepared, not yet activated)

The Research Service also has a **Research-owned** outbound WebSocket adapter
(`edge_runtime.rs`) behind the explicit `--edge-runtime` mode. It connects
with `hello_research`, the `research` runtime profile and a distinct bearer;
it binds requests and replies to an authenticated Edge session generation.
It does not import Engine's Cloudflare transport, private account credentials,
financial storage or exchange order-sending capability. This mode uses the
already implemented `IndependentOkxPublicSource` rather than Engine's public
market callback, and bounds frame size and concurrent work. The existing
Engine-hosted IPC mode remains unchanged.

Supported direct operations: Research calculations/jobs, explicit-instrument
Market requests, MarketCapabilities, PortfolioScenario, and read-only
Research job diagnostics. FindInstruments, MarketOverview and
MarketMicrostructure depend on public cache/reference behavior still owned by
Engine; the independent endpoint currently returns a typed unavailable error
for these, **not** fabricated data or a financial Engine fallback. A Research
deployment diagnostic also returns `RESEARCH_DEPLOYMENT_NOT_SUPERVISED` until
CONTROL can attest the standalone process instead of reusing the old Engine
child's deployment snapshot. A Research socket refuses trading, account
selection, strategy mutation, risk commands
and policy snapshot frames. Edge's internal `target_owner` routing marker is
removed before constructing the strict native Rust request.

The standalone mode now has an **opt-in CONTROL code path**:

- `StartResearch` requires the exact SHA-256 of the installed, reverified
  content-addressed worker. CONTROL starts it in a **separate Windows Job**,
  sends a bounded startup receipt only after Job assignment, and persists an
  independent process generation. Research rejects startup if that receipt
  is absent or invalid; it does not race its own memory Job against CONTROL.
  Inside that process the Worker installs and reads back a nested Windows Job
  with a **4 GiB memory limit and 50% CPU hard cap**. Both are fail-closed:
  inability to enforce either limit prevents Research startup. During this
  transition CONTROL admits standalone Research only with Engine stopped and
  rejects an Engine start while standalone Research is desired or owned.
  Conflicting desired states after restart cannot launch either process.
  Simultaneous Engine and Research operation follows retirement of the Engine-owned
  Research child. Windows environments with DFSS need physical verification of
  the CPU rate control.
- `StopResearch` requires the active worker SHA and matching process generation;
  status/desired state live in CONTROL's own `research-state.json` rather than
  Engine's `engine-state.pb`. The standalone SQLite catalog is rooted at
  `RUNTIME_ROOT/research/edge-catalog`, not the Engine child catalog. These
  catalogs are deliberately **not** automatically merged.
- `ProvisionResearchRuntimeToken` generates a separate secret and passes it to
  the verified Research executable by bounded stdin. Research writes it to
  its own Windows Credential Manager target; the CONTROL response contains
  only its SHA-256, never the bearer. Provisioning while Research is desired
  or observed running is rejected: the deliberate rotation order is **stop,
  provision, configure the matching Edge hash, start, authenticate and read
  back**. Windows standalone mode ignores bearer environment variables and
  pins the allowed WSS endpoint.
- `StatusResearch`, including the read-only MCP Bridge tool `research_status`,
  is **process evidence only**. Its `RUNNING_UNVERIFIED` phase does not mean an
  authenticated Edge session, loaded job catalog, or successful market read.

This opt-in code path is subject to Windows CI and subsequent physical
acceptance, **not** an assertion that the installed product has changed mode.
Do not enable Edge's `RESEARCH_RUNTIME_TOKEN_SHA256` merely because the
executable can connect: a configured but offline Research owner fails closed.
Independent Edge token registration, authenticated-session readback, clean
restart/reboot, job compatibility, independent update and rollback evidence
are still required before cutover. Until that accepted cutover, the installed
product continues to use the Engine child and its Engine-owned source.
Separate Credential Manager targets and distinct crates are a code-level
least-authority measure, **not** an OS security boundary when both processes
run as the same Windows user.

Agent has no production dependency on the research implementation or worker.
Worker has no production dependency on Agent, execution, API credentials, or
financial storage. Its test-only HTTP fixtures do not authorize those edges.
`scripts/check_architecture.py` checks both direct and transitive dependencies.

Research requests currently receive public observations through the Engine's
host source owner (`ResearchClient::installed` delegates to
`MarketBootstrapper::research_source`). They cannot issue arbitrary HTTP,
access an account, reserve money, or place orders. **This is an existing
runtime dependency, not the desired independent Research boundary.**

The Research executable can also serve the existing framed commands with
`--independent-source --root <research-dir> --scope <scope>`. Its local
provider reads public OKX candles, recent and historical trades, SWAP
reference data, tickers, historical funding and open interest. It rejects parent source replies
in that mode. Use `--market-request --root <research-dir> --scope <scope>`
to read one JSON `MarketRequest` from stdin and print the result. Explicit
instruments are required. These requests use the same analytics, coverage
and artifact code as hosted requests. Engine still resolves instrument
universe queries on its existing hosted route.

The live strategy turn in `okx-trading-engine/src/trade_service/strategies.rs`
captures closed prices from Engine's public source. With a compatible worker
it stages the captured input in Research and checks for a committed proposal
on a later strategy turn. Research evaluates the signal in the background
and commits it to its SQLite outbox before returning it. A worker restart
keeps committed proposals until expiry and drops pending computations.
Workers with the previous protocol continue to use `EvaluateSignal`.

Engine checks the proposal against its installed strategy, its own digest
of the exact input, the policy revision and current time. The existing
financial SQLite ledger serializes duplicate claims. RiskBook and Engine
retain all admission and venue mutation authority. Manual trade commands
and reconciliation continue independently of the strategy lane. A fully
autonomous Research scheduler would also need to receive strategy
configuration and capture its own input independently of Engine.

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

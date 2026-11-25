# Native CI ownership and build boundaries (#537)

The protected checks `classify`, `linux-core` and `windows-native` stay required. They
aggregate different **executed** jobs; a skipped financial matrix is not
reported as a successful financial test run.

**One automatic CI for `main`:** `.github/workflows/ci.yml` runs the protected
checks and, only after their success, the optional Native-base publication job.
No second `workflow_run` is started on completion. GitHub Actions has separate
manual operational workflows for protected account access and Cloudflare
deployment; those operations do not appear as permanently skipped CI jobs.
The historical All26 branch-only workflow and 26 unreachable JS adapters
were **deleted** in merged PR #593; the exact 26-ID archive and offline
negative verifier remain as historical evidence. Explicit manual recovery
operations are outside the normal automatic `main` CI path. Closed #510/#512
migration jobs were removed from the main workflow.

| Changed files (complete GitHub diff) | Direct owner and consumers | Ready CI | Native release artifact |
| --- | --- | --- | --- |
| Only `crates/okx-host-launcher/src/**/*.rs` (+ docs) | Launcher, plus Host Controller consumer | `host-controller-launcher-checks`; no Trading Engine/SQL compilation | None: host identities are not an installable four-component bundle |
| Only `crates/okx-host-controller/src/**/*.rs` and optional Launcher Rust source (+ docs) | Host Controller and Launcher | `host-controller-launcher-checks`; no Trading Engine/SQL compilation | None |
| Trading Engine/TradeService, `okx-trade-execution` or financial SQL | Trading Engine, RiskBook, SQL and trade-side consumers | Mandatory financial `trading-engine-acceptance-windows`, independent `trading-engine-regressions-windows`, parallel `trading-engine-build-windows`, B1 and Linux financial regressions | Full verified Windows bundle on qualifying PR |
| Shared `okx-api`, `okx-market-runtime`, `okx-ipc-contracts`, root Cargo files, build scripts, installer/packaging scripts or CI workflow | All affected consumers | Full Native matrix (fail closed) | Full only after acceptance |
| Narrow Research/Host `Cargo.toml` with optional `Cargo.lock` | Only that owner and its actual consumers | Scoped **only after exact before/after Windows Cargo closure proof**, including versions/features/dev dependencies; otherwise full Native | Research-only standalone three-file component when its exact owner scope qualifies; host-only nondeployable evidence otherwise |
| Research-only owner changes; unchanged Engine/Host release closure | Research/IPC consumers | Standalone Research Windows gate and independently verified Cargo closure, exact Ready PR attempt and required aggregate jobs | Raw `okx-research-component-<PR_HEAD_SHA>` CI artifact: only `Cargo.lock`, `manifest.json` (`okx.windows.research-component/v1`), `okx-research-service.exe`; append-only `research-component-<MERGE_SHA>` after verified merge, no Engine files |
| Documentation only / Cloudflare-only | Documentation / MCP | Existing scoped gates | No fresh Native build |
| Mixed Host + Research with exact independently proven Windows closures | CONTROL and Research | Both scoped Windows owners' checks; no Engine build | Separate component evidence, no combined unsafely repacked release |
| Host + Cloudflare source, when each scope is independently proven | CONTROL + Edge | Host Windows checks and Cloudflare contracts | No Engine rebuild |
| Shared wire, unknown path, cross-owner ambiguity, renames or incomplete diff | Potentially all | Full Native matrix | Only if accepted |

The host-only source exception remains intentionally **narrow**: just Rust
implementation files inside the two host crates. A Host or Research manifest
change takes the corresponding scoped route **only if a separate resolver proof
establishes** that all protected Windows dependency closures are identical.
Unverified dependency changes and installation contracts stay full. The
`classify` job verifies the complete comparison of the immutable event
base and head. A failed, capped or ambiguous comparison selects full impact.
Draft executes the checks emitted by this same job without fetching Git
history or classifying the diff again. Ready can narrow a manifest candidate
only after the separate Cargo closure proof.
The job summary records the selected scope and reason.
The same self-test checks that the shell scope guard accepts exactly the
classifier's output tokens; it runs on Draft PRs before Rust compilation.

`scripts/check_architecture.py` separately rejects direct and transitive
financial/market dependencies from CONTROL or Launcher, as well as a Launcher
dependency on CONTROL. CONTROL may depend on Launcher and the versioned
protocol. The financial SQLite owner remains in `okx-trade-execution` and its
TradeService/Engine consumers remain in `okx-trading-engine`; this stage does **not**
claim separate financial SQL and Engine release cycles.

For a full Native change, `windows-native` requires financial acceptance,
all Windows regression suites and the Windows release build to succeed.
The complete budgeted financial collector and isolated OS/SQL/process fixtures
remain on one runner. The `okx-native-finance-check` CI tool builds and tests
its own validation code without linking Engine, then runs the actual Cargo
test binaries for the verified source tree. The regression job runs four
parallel suites for Engine, Host, Trade Execution and Research. Native Clippy
with `--all-targets` and `-D warnings` runs after Host tests on that runner.
The three package suites derive their financial-witness exclusions from the
collector manifest. Engine's SQLite writer and source-frontier fixtures retain
their serial test settings. Research runs its library tests, IPC process tests
and K9 in its own suite. The release build uses the same source tree.
Publication verifies the success of every suite.

The four serialized, clock-sensitive Trading Engine witnesses also record nine
monotonic lifecycle phase durations each in the existing financial JSON report
(`clock_sensitive_phase_timings`). The collector requires exact current-run
receipt identities and preserves its original resource-budget and witness
checks; these timings diagnose slow setup, entry, settlement, release or cold
reopen, but are not independent proof of financial correctness.

The host job runs the existing installation-root safety fixture, architecture
guard, formatting, clippy, Rust tests and Windows release builds for both
CONTROL and Launcher. Its exact source HEAD, tree, component scope and binary
hashes are retained as **non-deployable** evidence. It never publishes a new
Native base; `publish-accepted-windows-release` inside the **same** protected
`.github/workflows/ci.yml` runs after successful main checks and continues
to require fresh financial acceptance, all four regression suites (including
Host Clippy) and a complete four-component Windows artifact. It publishes a
GitHub release but never
installs or restarts the Trading Engine.

The accepted `main` run reuses the exact successful Ready PR artifact without
compiling Engine again. Only a manual `workflow_dispatch` on accepted `main`
with `prime_windows_cargo_cache=true` starts cache warming. The
`cache_profile=tests|release|both` choice starts one or two Windows profile
runners (default `both`); each compiles only on a cache miss. Their results
sit outside the protected acceptance and publication gates. Test builds omit debug
symbols while preserving debug assertions. The release build retains its
Cargo profile. PR jobs restore caches from trusted main and compile their
own source tree with Cargo.

Draft PRs with only host Rust sources run direct Launcher and CONTROL Cargo
tests of library and binary targets via `scripts/check_p0.py`. Research-owned
IPC implementation changes run Research tests and its child-process contracts.
Shared wire changes still exercise the consumers. Manifest and CI changes
retain the expanded Draft checks.

Acceptance controls:

1. A CONTROL-only or Launcher-only Rust diff selects its own Windows job;
   that job compiles both host owner/consumer crates, never Agent/Execution.
2. A shared IPC, Cargo manifest, SQL or Engine diff selects full financial CI.
3. A mixed Host+Research or Host+Edge diff must run **both** proven owner
   routes; it cannot inherit the cheap Host path alone. Unknown/shared
   cross-owner impact always takes the full financial route.
4. Existing protected aggregate names remain, and the host-only branch fails
   if its specific Windows job fails or is skipped.
5. Changes to these classifier/workflow/architecture rules themselves select
   `full`; the first adoption therefore requires existing full acceptance.

Measured elapsed times and the possible future separation of Engine vs SQL
test jobs remain open acceptance work, not completed by this routing change.

## Cargo consumer inventory (#560, diagnostic phase)

The existing `dependency-audit` job already runs `cargo metadata --locked`. The
same resolved graph is now reused by `scripts/ci-component-impact.cjs` to record
which deployable Windows binaries consume each changed workspace crate via
normal/build dependencies. The Research release closure excludes
`okx-engine-domain`. Research-protocol/domain changes are isolated from the
Engine/CONTROL/Launcher/Bridge Windows normal/build/dev closures as of #595;
the classifier must prove this for each new candidate. A Launcher
implementation change reaches the Host Controller. Dev-only dependencies are excluded from **release** impact,
not from the tests that may need to run.

The report lists actual dependency paths, affected binary names and separate
Cloudflare changes. It is retained in the existing dependency-audit artifact
and GitHub job summary; no new CI workflow, Rust compilation or Windows runner
is introduced for inventory. The old statement that Research protocol still
reaches Engine through `okx-ipc-contracts` is obsolete after PR #595: the
strict Windows normal/build/dev boundary is now checked on current HEAD.
Any Cargo manifest, lock, build script, unknown
path, unverified rename or incomplete diff is reported as potentially affecting
all Windows binaries. An unavailable push diff is also explicitly unverified.

**The inventory is diagnostic-only, not a skip authorization.** The existing
fail-closed classifier and immutable-release checks still select tests and
builds. Narrow manifest candidates pass through a *separate* pre/post resolver
in the existing `classify` job: both Cargo versions use the same pinned toolchain,
`--locked`, and `--filter-platform x86_64-pc-windows-msvc`. The proof compares
the resolved package identities, checksums, enabled and declared features,
dependency aliases/kinds/targets and crate target definitions across complete
protected transitive closures. It conservatively includes dev dependencies to
protect Windows tests. A missing node, mixed owner, unsupported path, changed
protected dependency, failed resolver or stale PR HEAD/base means **full**.
No separate workflow or Windows runner is added to classify the change.

For a **Research-only** change, no Native-base ZIP is fetched or composed.
The standalone Windows owner job builds the Research executable only, checks
the strict three-file `okx.windows.research-component/v1` manifest and uploads
the raw `okx-research-component-<PR_HEAD_SHA>` PR CI artifact. CONTROL accepts
it only after checking the exact successful Ready run **attempt**, required
job results, non-expired artifact digest, and equal GitHub tested/main trees.
After a source-identical merge, main CI may publish the identical ZIP with
`published-research-origin.json` and `SHA256SUMS.txt` under the immutable,
append-only `research-component-<MERGE_SHA>` tag; that release does not install
or start anything. No Engine, SQL, CONTROL or Launcher executable is inside
the Research-only artifact. The *installed prerequisites* are different:
CONTROL and Launcher must already be verified and active; a virgin Windows
bootstrap and Scheduler path remain **NOT_PROVEN**. Mixed-owner or changed
protected closures still require full Native acceptance. Positive/negative
manifest proof, accepted Windows CI receipts, release publication and physical
deployment are separate gates, never inferred from these source instructions.

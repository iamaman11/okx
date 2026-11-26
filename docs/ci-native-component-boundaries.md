# Native CI ownership and build boundaries (#537)

Branch protection requires only `ci`. `select-checks` classifies changed
owners; `ci` directly verifies every applicable owner job and dependency,
architecture, portable Rust and Edge check, including expected `skipped`
results on routes that must not build Engine. There are no separate
`linux-core` / `windows-native` aggregators or synthetic success aliases.
Skipped financial tests are never reported as executed financial tests.

**One automatic CI for `main`:** `.github/workflows/ci.yml` runs the protected
checks and, only after their success, the optional Native-base publication job.
No second `workflow_run` is started on completion. GitHub Actions has separate
manual operational workflows for protected account access and Cloudflare
deployment; those operations do not appear as permanently skipped CI jobs.
The historical All26 branch-only workflow and explicit recovery promotion
remain separate from the normal `main` CI path. Closed #510/#512 migration
jobs were removed from the main workflow.

| Changed files (complete GitHub diff) | Direct owner and consumers | Ready CI | Native release artifact |
| --- | --- | --- | --- |
| Only `crates/okx-host-launcher/src/**/*.rs` (+ docs) | Launcher, plus Host Controller consumer | `host-windows`; no Trading Engine/SQL compilation | None: host identities are not an installable four-component bundle |
| Only `crates/okx-host-controller/src/**/*.rs` and optional Launcher Rust source (+ docs) | Host Controller and Launcher | `host-windows`; no Trading Engine/SQL compilation | None |
| Trading Engine/TradeService, `okx-trade-execution` or financial SQL | Trading Engine, RiskBook, SQL and trade-side consumers | Mandatory financial `engine-windows`, independent `trading-engine-regressions-windows`, parallel `trading-engine-build-windows` and portable Rust checks | Full verified Windows bundle on qualifying PR |
| Shared `okx-api`, `okx-market-runtime`, `okx-ipc-contracts`, root Cargo files, build scripts, installer/packaging scripts or CI workflow | All affected consumers | Full Native matrix (fail closed) | Full only after acceptance |
| Narrow Research/Host `Cargo.toml` with optional `Cargo.lock` | Only that owner and its actual consumers | Scoped **only after exact before/after Windows Cargo closure proof**, including versions/features/dev dependencies; otherwise full Native | Research-only standalone three-file component when its exact owner scope qualifies; host-only nondeployable evidence otherwise |
| Research-only owner changes; unchanged Engine/Host release closure | Research/IPC consumers | `research-windows` and independently verified Cargo closure, exact Ready PR attempt and `ci` | Raw `okx-research-component-<PR_HEAD_SHA>` CI artifact: only `Cargo.lock`, `manifest.json` (`okx.windows.research-component/v3`), `okx-research-service.exe`; append-only `research-component-<MERGE_SHA>` after verified merge, no Engine files |
| Documentation only / Cloudflare-only | Documentation / MCP | Existing scoped gates | No fresh Native build |
| Mixed Research + Host with proven unchanged Engine dependency closure | Research and Host consumers | `research-windows` and `host-windows`, no Engine compilation | Separate Research/Host evidence; not a replacement Native base |
| Unknown/shared impact, unsafe mixed changes, renames, incomplete diff | All potentially affected consumers | Full Native matrix (fail closed) | Only if accepted |

Research coverage is monotonic across owner combinations: adding Host source
changes to an otherwise Research-only PR still requires `cloudflare-mcp` and
the live Research MCP/DO/WSS loopback witness in `research-windows`. It does
**not** authorize creation of a Research-only standalone release artifact from
mixed-owner changes. Negative routing mutations cover both requirements.

The host-only source exception remains intentionally **narrow**: just Rust
implementation files inside the two host crates. A Host or Research manifest
change takes the corresponding scoped route **only if a separate resolver proof
establishes** that all protected Windows dependency closures are identical.
Unverified dependency changes and installation contracts stay full. The
`select-checks` verifies the complete comparison of the immutable event
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

For a full Native change, `ci` requires financial acceptance,
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
Every selected Engine/Host/Execution Cargo package must report at least one
actually passed test, even if Cargo exits successfully; zero-test binary
targets are allowed provided another target in the package executed tests.
The Full Windows Research matrix separately requires all ten real-process IPC
cases (zero ignored/filtered), matching the owner-only Research gate. These
checks reuse existing jobs and do not add a matrix cell or runner.

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
compiling Engine again **or rerunning the dependency audit**. The required
`ci` gate verifies that the main-side audit is skipped only when the immutable
tree matches a successful, first-attempt Ready gate that included the real
dependency audit; new or unverified trees still require a fresh audit.
Both publishers require this explicit skipped result, successful `ci` and
the same exact Ready provenance before releasing any accepted bytes.

The Host-only runner now restores the existing **trusted main Engine-test
Cargo cache** read-only. Its debug/test profiles match that seed, which
already compiles Host Clippy and test dependencies. It builds only the two
Host release binaries from the changed source and never compiles Engine;
it no longer writes an isolated ~390 MB cache per Host PR. The cache is
not guaranteed to hit when Cargo.lock/toolchain/profile inputs change.
Release compilation and cross-PR warm speed still need a real Host-only
Ready measurement; avoiding a cache write alone does not prove speedup.

Only a manual `workflow_dispatch` on accepted `main`
with `prime_windows_cargo_cache=true` starts cache warming. The
`cache_profile=tests|release|research|both` choice starts one Windows profile
runner for an individual profile, or two Engine runners for `both` (default);
each compiles only on a cache miss. The independent `research` profile seeds
Research-only Clippy, test and release outputs with the same Cargo debug and
cache fingerprint as the `research-windows` PR runner. That PR runner is
read-only for the reusable cache; GitHub PR merge-ref caches do not warm sibling
PRs. Cache-prime results sit outside the protected acceptance and publication
gates. Test builds omit debug
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
3. A mixed host and non-host diff cannot inherit the cheap host path.
4. `ci` directly rejects failed or unexpectedly skipped `host-windows`,
   with no extra aggregate job.
5. Changes to these classifier/workflow/architecture rules themselves select
   `full`; the first adoption therefore requires existing full acceptance.

Measured elapsed times and the possible future separation of Engine vs SQL
test jobs remain open acceptance work, not completed by this routing change.

### First-attempt baseline before removing the two aggregators (#599)

| Existing Ready run | Workflow elapsed | Successful job runner time (sum) | Critical Windows owner |
| --- | ---: | ---: | --- |
| [Full #598](https://github.com/iamaman11/okx/actions/runs/45713166028) | 7m 08s | ~28.4 runner-min | `trading-engine-acceptance-windows`: 6m 46s |
| [Research #597](https://github.com/iamaman11/okx/actions/runs/45710343540) | 6m 39s | ~7.8 runner-min | `research-service-tests-windows`: 6m 20s |

Those figures come from the immutable run and individual GitHub job
`started_at`/`completed_at` timestamps; skipped jobs contribute no runner time.
The baseline contains `linux-core` and `windows-native` aggregation (2–3s
each), so deleting them primarily removes scheduling hops and two Ubuntu
runner allocations; it cannot by itself remove the Windows compilation on the
critical path. Compare first-attempt owner routes on the **same code scope**
after this change before claiming a measured wall-clock or cost improvement.

## Cargo consumer inventory (#560, diagnostic phase)

The existing `dependency-audit` job already runs `cargo metadata --locked`. The
same resolved graph is now reused by `scripts/ci-component-impact.cjs` to record
which deployable Windows binaries consume each changed workspace crate via
normal/build dependencies. The Research release closure excludes
`okx-engine-domain`. Changes to `okx-research-domain` still reach the Engine
through the existing Research route in `okx-ipc-contracts`. A Launcher
implementation change reaches the Host Controller. Dev-only dependencies are excluded from **release** impact,
not from the tests that may need to run.

The report lists actual dependency paths, affected binary names and separate
Cloudflare changes. It is retained in the existing dependency-audit artifact
and GitHub job summary; no new CI workflow, Rust compilation or Windows runner
is introduced for inventory. Any Cargo manifest, lock, build script, unknown
path, unverified rename or incomplete diff is reported as potentially affecting
all Windows binaries. An unavailable push diff is also explicitly unverified.

**The inventory is diagnostic-only, not a skip authorization.** The existing
fail-closed classifier and immutable-release checks still select tests and
builds. Narrow manifest candidates pass through a *separate* pre/post resolver
in the existing `select-checks` job: both Cargo versions use the same pinned toolchain,
`--locked`, and `--filter-platform x86_64-pc-windows-msvc`. The proof compares
the resolved package identities, checksums, enabled and declared features,
dependency aliases/kinds/targets and crate target definitions across complete
protected transitive closures. It conservatively includes dev dependencies to
protect Windows tests. A missing node, mixed owner, unsupported path, changed
protected dependency, failed resolver or stale PR HEAD/base means **full**.
No separate workflow or Windows runner is added to classify the change.

For a **Research-only** change, no Native-base ZIP is fetched or composed.
The standalone Windows owner job builds the Research executable only, checks
the strict three-file `okx.windows.research-component/v3` manifest and uploads
the raw `okx-research-component-<PR_HEAD_SHA>` PR CI artifact. CONTROL accepts
it only after checking the exact successful Ready run **attempt**, required
job results, non-expired artifact digest, and equal GitHub tested/main trees.
The deploy-side CONTROL pre-activation path first verifies the archive SHA-256
and typed three-file manifest, then selects its explicit v1/v2/v3 Ready-job
roster. Research v3 is **not** subjected to the retired seven-name v1 roster;
successful Engine/Host/matrix jobs remain forbidden on Research-only releases.
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

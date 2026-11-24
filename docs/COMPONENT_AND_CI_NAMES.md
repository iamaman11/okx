# Canonical component and CI names (#537)

Names describe **ownership**, not historical implementation details. A Cargo
package is a compilation boundary, a Windows executable is a release artifact,
and a CI job is evidence. They are not interchangeable.

## Rust packages

| Canonical Cargo package / directory | Previously called | Responsibility |
| --- | --- | --- |
| `okx-trading-engine` | `okx-agent` | Trading process: accounts, positions, financial decisions, exchange sends and recovery |
| `okx-trade-execution` | `okx-execution` | Engine-owned RiskBook, finance/SQLite and order execution; **not** another process |
| `okx-domain-core` | `okx-kernel` | Shared deterministic domain facts, mathematical rules and plans; no I/O |
| `okx-ipc-contracts` | `okx-protocol` | Versioned typed messages, command contracts and authenticated IPC |
| `okx-market-runtime` | `okx-runtime` | Runtime market sources, venue API, WebSocket and observation integration |
| `okx-research-service` | `okx-research-worker` | Separate research process and IPC implementation; no trading authority |
| `okx-host-controller` | `okx-host-control` | Windows installation, version activation, rollback, lifecycle and diagnostics |
| `okx-host-launcher` | unchanged | Verified native Windows process startup |
| `okx-strategy-core` | unchanged | Research-owned pure strategy calculations |
| `okx-research`, `okx-analysis` | unchanged | Research and analytical functions |

Inside those packages, `lifecycle_executor.rs` is the Host Controller's
operation runner; `analytics/cpu_executor.rs` is Research's bounded CPU
scheduler. They are not financial executors. The account authorization role
`Executor` is a separate, existing permission and is not another executable.

## Installed Windows compatibility identities

Renaming the Cargo package does **not** rename an installed service or the
artifact it consumes. Until a separate, explicitly verified controller/host
migration, the v2 manifest and installed installation keep these names:

| Process responsibility | Current artifact filename / compatible identity |
| --- | --- |
| Trading Engine (`okx-trading-engine`) | `okx-agent.exe`; `okx-agent.stdout.log`, `okx-agent.stderr.log` |
| Research Service (`okx-research-service`) | `okx-research-service.exe` |
| Host Controller (`okx-host-controller`) | `okx-host-control.exe`; `iamaman11-okx-host-control` task/mutex |
| Host Launcher | `okx-host-launcher.exe` |

The existing non-Windows default data directory remains `.okx-agent`, and
the Windows scheduled-task XML remains
`C:\okx\control\okx-host-control-task.xml`.

Windows Credential Manager targets are also persistent installation identities,
**not** Cargo package names. An installed token or signing key must remain
readable after a source-only rename:

| Owner | Stable Credential Manager service |
| --- | --- |
| Trading Engine identity/signing key | `iamaman11.okx-agent` |
| Trading Engine GitHub mailbox token | `iamaman11.okx-agent.github` |
| Trading Engine Cloudflare runtime token | `iamaman11.okx-agent.cloudflare` |
| Trading Engine account credentials | `iamaman11.okx-agent.account.<reference>` |
| Host Controller GitHub token | `iamaman11.okx-host-control.github` |

Any future credential target migration requires explicit, separately accepted
secret-preserving migration and rollback; never silently write a renamed target.

Historic release manifests, stored source SHA256, previously installed roots
(`C:\okx-runtime`) and other provenance records are facts, not identifiers to
rewrite. Do not silently rename them or fake a new accepted binary. There is
one financial SQLite owner **inside** Trading Engine, not a SQL service.

The accepted **Windows binary baseline** (formerly called “Native base”)
means proven reusable binary bytes, not a runtime library. Existing
`native-base-<sha>` release tags remain immutable compatibility identifiers.

## CI job names

| Job | Purpose |
| --- | --- |
| `classify` | Protected fail-closed changed-file boundary plus before/after Windows Cargo closure proof for narrow Research/Host manifest candidates |
| `draft-component-tests` | Scoped feedback for Draft pull requests |
| `trading-engine-acceptance-windows` | Mandatory complete financial collector/resource-budget acceptance and isolated Engine, SQLite, clock/process recovery tests |
| `trading-engine-regressions-windows` | Parallel independent Engine/Execution/Host/Research regressions, Windows Clippy and K9; never substitutes for financial acceptance |
| `trading-engine-build-windows` | Parallel fresh four-component Windows release build and artifact on full Ready PRs; manual accepted-`main` release cache seed |
| `trusted-main-windows-test-cache-prime` | Optional manual compile-only test/collector cache seed from accepted `main`; no financial acceptance or deployment |
| `trading-safety-acceptance-windows` | Independent mandatory B1 financial safety checks |
| `research-service-tests-windows` | Research Service tests and build against the accepted unchanged Trading Engine |
| `host-controller-launcher-checks` | Host Controller and Host Launcher tests/build without the Trading Engine |
| `rust-tests-linux` | Rust library, Research Service IPC and financial SQLite tests executed on Linux (not a Linux trading service) |
| `architecture-contracts` | Dependency, architecture, product and acceptance contract guards |
| `dependency-audit` | Dependency security, licence and origin checks |
| `cloudflare-mcp-contracts` | MCP transport and public contract checks |
| `research-package-proof` | Draft proof of a composed accepted Research candidate, not a deploy |
| `publish-accepted-windows-release` | After successful protected main CI, publish only the exact verified Windows artifact; no installation |

The required check **display names** `classify`, `windows-native` and `linux-core`
are retained for branch protection and accepted-CI reuse. `classify` is the
actual `component_impact` job, without a redundant alias; the platform
aggregators keep implementation IDs `required_windows` and `required_linux`.
They require finance acceptance, independent regressions, B1 and release build on the
full Native route; on reuse they verify the accepted exact tree instead. They
never claim that skipped financial tests ran. The old branch-only Draft jobs for closed
issues #510 and #512 have been removed; normal Draft owner tests remain.

## Release acceptance for a future executable rename

1. Keep binary names and v2 archive/manifest schema stable in this code-name
   migration; compile and test the renamed Cargo packages and both old EXE names.
2. Require the full Windows finance/B1, Linux, architecture and MCP acceptance
   once for this shared-dependency/workflow change. The Draft route is scoped.
3. Verify the exact immutable HEAD/tree, all four EXE hashes, and the installed
   CONTROL/Launcher/Engine/Research compatibility before deploying anything.
4. A later Windows-identity migration needs a typed, rollback-capable
   controller transition and explicit approval. Never manually replace the
   installed Engine, restart trading or repack mixed provenance.
5. After branch protection is migrated to new aggregate display names, remove
   the old CI aliases in their own independently checked change.

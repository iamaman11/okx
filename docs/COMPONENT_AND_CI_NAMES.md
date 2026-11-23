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
| `component-impact` | Fail-closed changed-file/Cargo boundary classifier |
| `draft-component-tests` | Scoped feedback for Draft pull requests |
| `trading-engine-windows-full` | Full Trading Engine/SQLite/finance Windows acceptance and four-EXE bundle |
| `trading-safety-windows-prerequisite` | Separate B1 financial-safety prerequisites |
| `research-service-windows` | Fresh Research tests/build with accepted unchanged Engine baseline |
| `host-lifecycle-windows` | CONTROL and Launcher owner/consumer checks, no Engine build |
| `trading-rust-linux` | Portable Rust and native financial regression suite |
| `architecture-contracts` | Dependency, architecture, product and acceptance contract guards |
| `dependency-audit` | Dependency security, licence and origin checks |
| `cloudflare-mcp-contracts` | MCP transport and public contract checks |
| `verified-research-bundle-assembly` | Draft proof of a composed accepted Research candidate, not a deploy |
| `draft-trading-migration-512` | Branch-specific legacy #512 validation only |
| `draft-riskbook-migration-510` | Branch-specific legacy #510 validation only |

The required check **display names** `windows-native` and `linux-core` are
temporarily retained because existing branch protection and accepted-CI reuse
depend on those exact values. Their implementation IDs are `required_windows`
and `required_linux`: they aggregate and verify evidence, not necessarily run
the full Windows or Linux suite. Change branch-protection settings *after*
a verified parallel transition, never by silently losing the required checks.

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

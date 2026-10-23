# Implementation and acceptance: issue #398

The canonical specification is [#398](https://github.com/iamaman11/okx/issues/398); integration is [PR #399](https://github.com/iamaman11/okx/pull/399). Baseline: `1aacbc196db00b301e7eacb404d5db9b400c62e2`. [The previous roadmap](history/ROADMAP-before-398.md) is historical evidence.

| Work | Published implementation | Remaining acceptance |
| --- | --- | --- |
| W01 | Persistent account access, action-aware admission, transactional CAS and idempotency | Installed MCP checks |
| W02 | Independent account resources, verified UID and named credential references | Physical second-account routing |
| W03 | Addressed SQLite facts/projections, bounded actor, immutable intents, indexed queries; old ledger removed | Repeat final million-row fixture after schema freeze |
| W04 | Continuous source capture, lots, exact costs, NAV and indexed central reservations | Real exchange reconciliation and campaign costs |
| W05 | Unified manual/automatic execution, protected exits, durable unknown-POST handling and fault fixtures | Real protected OPEN/CLOSE cycles |
| W06 | Shared analytical sources, bounded CPU/cache, finite operators including downside volatility; recorded release benchmarks | Final release measurements and physical network workloads |
| W07 | Causal EMA/replay/grid and durable jobs | Installed multi-instrument research and two-instance checks |
| W08 | Seven strict public tools, pagination, unified diagnostics and transport regression fixtures | Final wire and installed MCP verification |
| W09 | Native Engine lifecycle, explicit clean initialization, backup/restore and obsolete host paths removed | Automatic addressed restore reconciliation implemented; matched installed ReleaseSet pending |
| W10 | Integrated Rust/Worker suites, strict Clippy and guards passed; exact executed counts and evidence are in acceptance.json | Final exact CI, Windows installation and physical acceptance pending; prepared release analytics thresholds passed |

This table records implementation, not release acceptance. Intermediate checkpoints remain undeployed. Unit tests do not establish physical multi-account routing or Real exchange cycles. Physical results must remain NOT_RUN or BLOCKED until evidenced.

The native/Worker frame is `okx.direct-transport.frame/v2`; the public product contract is `okx.product/v1`. Account access uses strict `okx.governance/v3`. Missing/corrupt installation state fails closed and never reads old financial or policy state. Account registration preserves other grants and keeps every registered Real account in the central risk census.

Evidence: [W05 integration](verification/398-w05.json), [SQL million-row measurements](verification/398-sql-million.json), [analytics measurements](verification/398-analytics.md). Analytics evidence identifies its component hashes and executable fixtures; it is not an installed ReleaseSet measurement. The new downside-volatility operator has targeted numerical, missing-data and existing-market-pipeline regressions, without adding DTOs, SQL, gateway or CONTROL operations.

W10 local code evidence is [acceptance.json](../acceptance.json). The exact release asset carries final installed/MCP/exchange observations after deployment; preinstallation NOT_RUN fields are deliberate. Logging rotation/retention/redaction/drain regressions and the final prepared analytics measurements are included.

# vNext migration

The integration branch builds each application from its own directory.
The original workspace provides the installed version and its migration
reference. Compatibility copies are retired when their last consumers switch.

| Owner | Current vNext coverage | Remaining transfer |
| --- | --- | --- |
| Research | Causal graph kernel, replay entrypoint, public OKX candle page adapter | Research service jobs and catalog, datasets, artifact lifecycle, other public sources, public REST and WS capabilities, remaining analysis and replay |
| Engine | Exact risk rules and the transactional execution/SQLite library, schemas and tests | Engine command entrypoint, order execution orchestration, reconciliation, private REST and WS, public facts needed for trading, credentials and deployment |
| Control | Independent Launcher workspace and Windows candidate build | Controller, installation and rollback, artifact provenance, operator policy migration |
| Edge | Standalone Worker and fixed wire contract tests | Cross-owner compatibility, operator policy and migration from the GitHub mailbox and Render bridge |

The API, observation and IPC packages are split by the operations and types
their callers use. Research and Engine first demonstrate independence.
Versioned integration derives from an explicit cross-application scenario.
The complete scope and acceptance evidence are tracked in
[#569](https://github.com/iamaman11/okx/issues/569).

# OKX

[#398](https://github.com/iamaman11/okx/issues/398) is the sole current implementation and acceptance plan. It supersedes #160 and the former staged Live gate. PR #397 is merged; its source tree is the baseline, not evidence that #398 is complete.

The delivery is one integration PR with buildable W01–W10 commits, one matched ReleaseSet and one Windows/MCP acceptance campaign. Intermediate commits must not be deployed.

The target is one Rust Engine with independently routed accounts and strategy instances, persistent account access, one SQL owner per account, one central risk owner, compact MCP tools and causal Rust analytics. Installation starts clean from verified account identity, credential references and current exchange state. Old application financial history, grants, STOP and obligations are neither read nor imported. Ordinary restart must retain the new system's state and must never initialize a replacement database.

The authorized physical campaign is Real, with total campaign loss including costs limited to USD 20. This is a cumulative loss budget; it is not a per-order notional limit. Physical execution follows W10 after the exact CI ReleaseSet is ready.

Implementation status and verification evidence are recorded in [the current roadmap](docs/ROADMAP.md). Historical notes are retained in [the documentation archive](docs/history/README-before-398.md) and impose no additional activation or migration prerequisites.

Workspace checks:

```powershell
$env:NO_PROXY = '127.0.0.1,localhost'
cargo fmt --all -- --check
cargo test --locked --workspace
python scripts/check_architecture.py
python scripts/check_ledger_sql.py
npm --prefix cloudflare/okx-mcp ci
npm --prefix cloudflare/okx-mcp run check
```

The full product-contract and physical-acceptance checks remain required by #398 before release acceptance.

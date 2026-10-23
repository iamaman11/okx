# OKX

[#398](https://github.com/iamaman11/okx/issues/398) is the sole implementation and acceptance specification. PR #397 is the merged baseline; previous staged Live gates are historical.

One Rust Engine routes independent accounts and strategy instances. Persistent account access, addressed account SQL owners, one central RiskBook, seven compact MCP tools and shared causal Rust analytics form the current product. Installation starts clean from current verified identity and credential references. Old application financial history, grants, STOP and obligations are neither read nor imported. Ordinary restart retains new-format state and never initializes a replacement database.

Delivery uses one integration PR, a matched CI ReleaseSet and one Windows/MCP acceptance program. Intermediate checkpoints are not deployed. The authorized Real campaign has a cumulative loss ceiling of USD10 including costs across accounts, strategies and restart; profits do not refill it. Fixture success and CI promotion do not establish physical acceptance.

- [Current architecture](docs/ARCHITECTURE_CONTRACT_V1.md)
- [Operator workflow](docs/chatgpt-operator-contract.md)
- [Defaults, transitions and install/backup/restore runbook](docs/PRODUCT_ACCEPTANCE.md)
- [Published checkpoints and remaining acceptance](docs/ROADMAP.md)
- [Preinstallation code acceptance](acceptance.json)
- [Final manifest attached to the exact accepted release](https://github.com/iamaman11/okx/releases/latest/download/acceptance.json)

The tracked manifest precedes physical deployment. The release asset supplies the final installed/exchange observations and exact binary hashes; NOT_RUN/BLOCKED never means PASS. Prior documents are retained in [history](docs/history/README-before-398.md) without imposing activation or migration prerequisites.

Run complete local code acceptance from PowerShell:

```powershell
$env:NO_PROXY = '127.0.0.1,localhost'
python scripts/acceptance_product.py --run
```

This runs format, architecture/SQL/product guards, locked Rust tests and Clippy, plus Worker checks. Exact CI and physical acceptance remain separate requirements.

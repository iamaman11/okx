# OKX — clean Engine

**[Issue #398](https://github.com/iamaman11/okx/issues/398) is the sole active requirements and acceptance source.** Its [acceptance and testing checklist](https://github.com/iamaman11/okx/issues/398#issuecomment-6373732525) is mandatory. Older Stage 5 and W01–W10 plans are historical, not prerequisites.

One Rust Engine routes independent AccountKey contexts, addressed SQLite WAL and a central governance/RiskBook. The public MCP has exactly seven typed tools: `accounts`, `strategies`, `market`, `research`, `trade`, `portfolio`, `diagnostics`. No Demo-only Agent, CONTROL profile handoff, v14 import or arbitrary shell/SQL/RPC route.

**Verified 23 October 2026:** PR [#403](https://github.com/iamaman11/okx/pull/403) is merged, Agent/Controller/Launcher are installed with `release_parity=MATCHED`, and central/native SQL campaign policy is USD10, revision 2. F03, F05, F07, F08 and final F09 remain open. This is not a Real-trade authorization. The second Real UID is absent: `PHYSICAL_MULTI_ACCOUNT=BLOCKED`. Consult #398 for newer facts.

- [Architecture and owners](docs/ARCHITECTURE_CONTRACT_V1.md)
- [Operator rules](docs/chatgpt-operator-contract.md)
- [Operational acceptance and CONTROL](docs/PRODUCT_ACCEPTANCE.md)
- [Observed requirement status](docs/ROADMAP.md) — not another plan
- [Historical non-normative evidence](docs/history/README-before-398.md)

```powershell
python scripts/acceptance_product.py --self-test
python scripts/acceptance_product.py --run
```

The checked-in [acceptance.json](acceptance.json) is **preinstallation code evidence**, not installed product proof. Final acceptance must cite the **specific immutable release tag and asset SHA256**, never a mutable latest-release URL. Do not reinitialize current SQL or reset the campaign while fixing F01–F09.

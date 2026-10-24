# OKX — clean Engine

**[Issue #398](https://github.com/iamaman11/okx/issues/398) is the sole active requirements and acceptance source.** Its [acceptance and testing checklist](https://github.com/iamaman11/okx/issues/398#issuecomment-6373732525) is mandatory. Older Stage 5 and W01–W10 plans are historical, not prerequisites.

One Rust Engine routes independent AccountKey contexts, addressed SQLite WAL and a central governance/RiskBook. The public MCP has exactly seven typed tools: `accounts`, `strategies`, `market`, `research`, `trade`, `portfolio`, `diagnostics`. No Demo-only Agent, CONTROL profile handoff, v14 import or arbitrary shell/SQL/RPC route.

**Last recorded physical proof, 24 October 2026:** [PR #416](https://github.com/iamaman11/okx/pull/416), exact [CI #40055850675](https://github.com/iamaman11/okx/actions/runs/40055850675) 6/6, native Windows artifact `12545318154` installed via CONTROL with Agent/Controller/Launcher `release_parity=MATCHED`. Live WebSocket snapshot, bounded market microstructure, read-only trade preview, fresh SQL sources and covered NAV windows have positive evidence. The USD10 central/native campaign ceiling revision 2 was preserved. **Independent Worker-side release attestation, full 16→7 business parity, concurrent strategy acceptance, two protected Real cycles and a second physical Real UID remain unaccepted.** Consult #398 for newer observations; no real trade authorization follows from this snapshot.

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

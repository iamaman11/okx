# One venue/account trading surface

> **Normative architecture:** [Architecture Contract v1](ARCHITECTURE_CONTRACT_V1.md). This file documents the first partial implementation; it is not a second architecture or proof of multi-account concurrency.

This is the canonical operator model for the existing OKX application.

```
venue + account_id -> Account Registry -> one CONTROL selection owner
                 -> one typed Cloudflare request -> one Rust execution/risk owner
                 -> verified exchange adapter -> exchange
```

- `venue` identifies the exchange. `account_id` identifies an exchange account;
  the account's Demo/Real environment and authenticated UID are **attributes**
  of the registry entry, never separate commands or executors.
- `select_account` is one typed operation with `venue` and `account_id`.
  For compatibility, the previous OKX-only wire shape defaults to `venue=okx`.
- `start_agent` starts the **already selected** account; it does not force Real.
  The legacy `start_demo_acceptance` / `restore_production_agent` operations are
  rejected. Existing CONTROL recovery/drain/Job/STOP remain the only owner.
- `execution_action` is the same typed tool on Demo and Real. The Worker is
  not a second policy authority. Native Rust rejects unavailable action
  capabilities and verifies permission/UID/STOP/source/RiskBook before POST.
  Removing a Worker-side operation allowlist **does not** permit a mutation
  denied by native Rust.
- `account_catalog` lists the implemented OKX selector slots and includes the
  exchange identity for named metadata entries. Old catalog files without a
  venue field are read as OKX. A metadata-only entry is **never selectable**
  until an exact credential/UID/native adapter/handoff acceptance exists.
- `bybit` is recognized as a typed identifier for extension but **has no
  installed adapter, provisioned credentials, or trading authority** and its
  selection fails closed.

Current limitation: The only physically implemented account routes are
`okx/real` and `okx/demo`. They still use the existing exclusive native
process handoff, not concurrent account execution. A universal multi-venue,
multi-account concurrent engine requires account-keyed execution contexts,
isolated ledgers and RiskBook reservations, native venue adapters and account
recovery; this PR does not claim those capabilities or any Real trade receipt.

Acceptance: complete Rust/Windows + Cloudflare CI, exact artifact installation,
native CONTROL selection regression for existing accounts, and independent
source/UID/ledger/STOP checks. Never re-post an UNKNOWN intent.

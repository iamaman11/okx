# Implementation and acceptance: issue #398

The only current plan is [#398](https://github.com/iamaman11/okx/issues/398). The source baseline is `1aacbc196db00b301e7eacb404d5db9b400c62e2` (merged PR #397). [The previous roadmap](history/ROADMAP-before-398.md) is historical evidence and does not prescribe migration, research or activation ceremonies.

| Work | Current state | Remaining acceptance |
| --- | --- | --- |
| W01 | In progress: account-scoped persistent access storage, atomic CAS/idempotent outcomes, protocol DTOs and removal of old governance conversion/delivery | Native connect and complete end-to-end action-aware admission |
| W02 | Account catalog, credential-reference verification and independent native account resources implemented | SQL-backed strategy instances are integrated in W03–W05 |
| W03 | Not implemented in this branch | Addressed SQL facts/projections; remove blob and historical ledger; pin and verify SQLite |
| W04 | Not implemented in this branch | Continuous finances, lots, campaign accounting and indexed central reservations |
| W05 | Not implemented in this branch | One execution command; verified exits under STOP; fault matrix |
| W06 | Not implemented in this branch | Shared sources/cache, bounded Rust operators and measured workloads |
| W07 | Not implemented in this branch | Parameterized causal EMA/grid and durable analytical jobs |
| W08 | Transport cleanup regression implemented alongside W01 | Complete public action surface, result pagination and diagnostics |
| W09 | Not implemented in this branch | Native clean-install lifecycle, remove old execution/host paths and matched ReleaseSet |
| W10 | Not run | Exact CI, Windows installation, analytics performance and Real exchange acceptance |

No row above is a release acceptance claim. Passing unit/fixture tests does not establish physical multi-account routing or Real cycles. Intermediate source changes must remain undeployed until W01–W09 form a coherent release.

The implemented account access owner uses `account_access_v1` with strict `okx.governance/v3`. It only initializes through the explicit installation function. Missing/corrupt state on a normal read is an error; old keys are never consulted. `enabled`, `explicit_pause`, limits and account policy revision survive storage reconstruction. Connecting another account retains the existing account grant. Configuration cannot remove a registered account from risk census. Access-operation keys retain a body hash and original transactional outcome.

The new native/Worker frame is `okx.direct-transport.frame/v2`; the old central-policy delivery DTO and fallback were removed. The remaining native host/execution epoch paths are still present and must be removed in their assigned W steps before deployment. Native identity verification and account-context routing are now wired to `accounts.connect` through a named credential reference. The opening SQL model and its initializer are replaced in W03/W09; this intermediate commit is not deployable.

Transport regression tests exercise actual RuntimeSession dispatch with simulated WebSocket delivery: disconnect before/after ACK, synchronous send failure, stale generation and duplicate request IDs. ACK and response share one observed completion and generation-scoped cleanup. These tests do not simulate an exchange POST and are not EXEC-01 acceptance.

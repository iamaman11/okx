## Need / failure

<!-- What concrete user capability, reproduced defect, external compatibility requirement, measured bottleneck, or simplification does this change address? -->

## Authoritative owner

<!-- Which existing layer owns the new responsibility? If a new owner is proposed, explain why no current owner can own it. -->

## Smallest capability delta

<!-- Describe the smallest implementation that closes the need. Avoid future-only/general-framework work. -->

## Architecture delta

- New Rust crates: 0
- New long-lived Tokio tasks: 0
- New mutable state owners: 0
- New persistent stores/databases: 0
- New schedulers/poll loops: 0
- New transport channels/services: 0
- New mutation authorities: 0
- New MCP tools: 0
- New third-party dependencies: 0
- New durable schemas/state files: 0

<!-- Explain every non-zero item. -->

## Evidence

<!-- For every nontrivial test: invariant/failure -> stimulus -> observable product evidence -> PASS condition. -->

## Superseded path

<!-- What old/duplicate path is removed or intentionally retained? State the removal condition for any compatibility shim. -->

## Simplicity review

- [ ] No duplicate state/formula/authority was introduced.
- [ ] No speculative future-only abstraction was added.
- [ ] Transport remains transport-only.
- [ ] The representative path can still be traced as protocol -> owner -> result.
- [ ] Tests protect product/architecture invariants rather than private helper structure.

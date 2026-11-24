# Engine financial core

This workspace contains the exact risk types and rules from the installed
version, and the execution library with its schema, SQLite owner and finance
tests. The execution library uses the local risk crate. The independent
`okx-trading-engine` crate is a thin S0 command-boundary candidate. It
validates transport values but **cannot** authorize a financial operation,
create a venue send claim or access private keys. Its offline `sql_handoff`
test deliberately stops at the original durable `AccountStore::prepare`.
The workspace checks run from a checkout of this directory alone.

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
cargo build --locked --release --workspace
```

The legacy source stays in place for the installed version during migration.
The vNext workspace currently builds libraries. A production trading command
entrypoint, independently sourced market/venue adapters, full risk admission,
committed one-time venue send and Windows B1/installation acceptance belong to
later slices. Green S0 command parsing or offline PREPARED-state tests must
never be reported as a live-trading or financial acceptance result.

The current two-crate financial core and three-crate candidate (adding the
thin command layer) must be compared on real command, SQL, risk and venue
diffs with compiler/link events and complete CI timings before choosing the
production composition. A separate SQL process is not an option.

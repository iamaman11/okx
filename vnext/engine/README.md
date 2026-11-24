# Engine financial core

This workspace contains the exact risk types and rules from the installed
version, and the execution library with its schema, SQLite owner and finance
tests. The execution library uses the local risk crate. The workspace checks
run from a checkout of this directory alone.

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
cargo build --locked --release --workspace
```

The legacy source stays in place for the installed version during migration.
The vNext workspace currently builds libraries. A trading command entrypoint,
venue adapters and Windows installation acceptance belong to later slices.

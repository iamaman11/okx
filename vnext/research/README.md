# Research replay

This workspace replays a checked, close-only causal graph from recorded closed
bars on standard input and emits a JSON array of timestamped results. It can be
built and tested from a copy of this directory alone.

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
cargo build --locked --release --bin okx-research-replay
```

The kernel is extracted from the installed version for the first vNext slice.
The legacy copy serves existing callers until their migration. New Research
work belongs in this workspace. The next public-source slice connects its own
provider to the Research entrypoint. Replay currently consumes a supplied
dataset and has no network or trading authority.

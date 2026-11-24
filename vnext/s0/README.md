# S0 local measurements

This small standard-library tool measures an explicitly chosen owner's build
and test commands for the two topology prototypes required by [#569](https://github.com/iamaman11/okx/issues/569).
It is **not** a Ready CI gate or proof of the ≤120-second requirement.

Run the same commands against two independently prepared, isolated workspace
directories (e.g. archive exports of two candidate revisions). The script
never deletes build caches or starts installation, Control or live trading. It
does not invoke a shell for the measured commands.

```sh
python3 -B vnext/s0/measure.py run \
  --repository . \
  --workspace /tmp/s0-variant-a/engine \
  --owner engine --name compact \
  --output /tmp/s0-compact.json \
  --command 'cargo test --locked --workspace' \
  --command 'cargo build --locked --release --workspace'

python3 -B vnext/s0/measure.py run \
  --repository . \
  --workspace /tmp/s0-variant-b/engine \
  --owner engine --name separated \
  --output /tmp/s0-separated.json \
  --command 'cargo test --locked --workspace' \
  --command 'cargo build --locked --release --workspace'

python3 -B vnext/s0/measure.py compare \
  --baseline /tmp/s0-compact.json \
  --candidate /tmp/s0-separated.json \
  --output /tmp/s0-comparison.json
```

Replace the workspace paths with actual exported candidate directories.
Research and Control accept the same Cargo commands; Edge accepts explicit
`npm ci`, `npm run check`, `npm test` or `node scripts/test-name.mjs` commands.
Use an owner-specific lockfile and comparable runner class, toolchains,
features, target, environment and cache policy. A changed SHA-256 of workspace
source inputs is **necessary but not sufficient**: independently document the
meaningful behavior-changing source diff and its failing-before/passing-after
oracle. Use actual isolated copies to avoid reading sibling source files.

Each JSON sample contains source fingerprint, Git HEAD/tree and dirty flag (if
`--repository` points at a checkout), platform/toolchain, Cargo metadata
closure, build-affecting environment input names (never secrets), phase durations,
Cargo artifact events, fresh/nonfresh counts, and
basic test summaries. `cargo --message-format=json` compiler-artifact events are
*not* exact rustc invocations; `rustc_verbose_invocations` is unknown without
Cargo verbose logs. Actual linker invocations are also **not instrumented**.
Do not treat executable artifacts as a one-to-one count of linker processes.

`compare` rejects unchanged source fingerprints, mismatched command lists or
toolchains and failed phases. It reports only sequential local phase deltas.
The full Ready budget requires separate evidence for real Windows/Linux
compiler/link/test events, finance/OS/B1 witnesses, production artifacts,
provenance, queues and the protected gate's `t0→t1` time as specified by #569.

Check the tool without Rust installed:

```sh
python3 -B -m unittest discover -s vnext/s0 -p 'test_*.py'
```

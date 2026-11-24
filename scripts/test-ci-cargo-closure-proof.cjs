'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {verify} = require('./ci-cargo-closure-proof.cjs');

const root = '/tmp/ci-head';
const base = '/tmp/ci-base';
const owners = ['okx-trading-engine', 'okx-trade-execution', 'okx-host-controller',
  'okx-host-launcher', 'okx-research-service', 'okx-research', 'okx-strategy-core',
  'okx-ipc-contracts'];
const external = name => `registry+https://github.com/rust-lang/crates.io-index#${name}@1.0.0`;
const edge = (pkg, kind = null, target = null) => ({name: pkg.split('#')[0], pkg,
  dep_kinds: [{kind, target}]});
const dependencies = {
  'okx-trading-engine': [edge('okx-trade-execution'), edge('okx-ipc-contracts'), edge(external('sqlite'))],
  'okx-trade-execution': [edge(external('serde'), 'dev')],
  'okx-host-controller': [edge('okx-host-launcher'), edge('okx-ipc-contracts')],
  'okx-research-service': [edge('okx-research'), edge('okx-strategy-core'), edge('okx-ipc-contracts')],
  'okx-research': [edge('okx-strategy-core')],
};
function metadata(workspaceRoot = root) {
  const externalNames = ['sqlite', 'serde'];
  const packages = [
    ...owners.map(name => ({id: name, name, version: '0.1.0', source: null,
      manifest_path: path.join(workspaceRoot, 'crates', name, 'Cargo.toml'),
      edition: '2024', rust_version: null, links: null,
      features: {default: []},
      targets: [{name, kind: ['lib'], crate_types: ['lib'], edition: '2024'}]})),
    ...externalNames.map(name => ({id: external(name), name, version: '1.0.0',
      source: 'registry+https://github.com/rust-lang/crates.io-index',
      checksum: name.repeat(4), manifest_path: `/tmp/registry/${name}/Cargo.toml`,
      features: {}, targets: [{name, kind: ['lib'], crate_types: ['lib']}]})),
  ];
  return {workspace_root: workspaceRoot, workspace_members: owners,
    packages, resolve: {nodes: packages.map(pkg => ({id: pkg.id,
      features: ['default'].filter(f => Object.hasOwn(pkg.features, f)),
      deps: dependencies[pkg.id] ?? []}))}};
}
const files = (...names) => [names.map(filename => ({filename, status: 'modified'}))];
const research = files('crates/okx-research-service/Cargo.toml', 'Cargo.lock');
const host = files('crates/okx-host-controller/Cargo.toml', 'Cargo.lock');
const head = metadata(), before = metadata(base);

assert.equal(verify(before, head, research, 'research').protected_closures_identical, true);
assert.equal(verify(before, head, host, 'host-controller').target, 'x86_64-pc-windows-msvc');
assert.equal(verify(before, head, files('crates/okx-host-controller/Cargo.toml', 'Cargo.lock',
  'cloudflare/okx-mcp/src/mcp.ts'), 'host-controller').protected_closures_identical, true);
assert.equal(verify(before, head, files('crates/okx-host-launcher/Cargo.toml'),
  'host-launcher').protected_closures_identical, true);

// A valid but unrelated Research version update leaves Engine/host binaries
// unchanged. Different worktree roots must not change the workspace IDs.
const researchOnly = structuredClone(head);
researchOnly.resolve.nodes.find(node => node.id === 'okx-research-service')
  .deps.push(edge(external('serde')));
assert.equal(verify(before, researchOnly, research, 'research').protected_closures_identical, true);

const reject = (mutation, scope = 'research', diff = research) => {
  const changed = structuredClone(head);
  mutation(changed);
  assert.throws(() => verify(before, changed, diff, scope));
};
reject(m => m.resolve.nodes.find(n => n.id === 'okx-trading-engine')
  .features.push('new_feature'));
reject(m => m.resolve.nodes.find(n => n.id === 'okx-trading-engine')
  .deps.push(edge(external('serde'), 'dev')));
reject(m => m.resolve.nodes.find(n => n.id === 'okx-trading-engine')
  .deps[0].dep_kinds[0].target = 'cfg(windows)');
reject(m => m.packages.find(p => p.id === external('sqlite')).checksum = 'other-sha');
reject(m => m.packages.find(p => p.id === 'okx-trade-execution').features.new = ['ipc']);
reject(m => m.packages.find(p => p.id === 'okx-trading-engine').targets[0]
  ['required-features'] = ['instrumented']);
reject(m => m.resolve.nodes.find(n => n.id === 'okx-trading-engine').deps[0].name = 'renamed');
reject(m => m.resolve.nodes.find(n => n.id === 'okx-host-controller')
  .deps.push(edge(external('serde'))));
reject(m => m.resolve.nodes.find(n => n.id === 'okx-trading-engine')
  .deps.push(edge('okx-research')));
reject(m => m.resolve.nodes.splice(m.resolve.nodes.findIndex(n => n.id === 'okx-trading-engine'), 1));
reject(m => m.packages.push({...m.packages[0], id: 'x', source: null}));
reject(m => m.resolve.nodes.find(n => n.id === 'okx-trading-engine').deps[0]
  .dep_kinds = []);
reject(m => m.resolve.nodes.find(n => n.id === 'okx-research-service')
  .deps.push(edge(external('sqlite'))), 'host-controller', host);
reject(m => m.resolve.nodes.find(n => n.id === 'okx-research-service')
  .features.push('research_change'), 'host-controller', host);

for (const diff of [
  files('Cargo.lock'),
  files('crates/okx-research-service/Cargo.toml', '.github/workflows/ci.yml'),
  files('crates/okx-research-service/Cargo.toml', 'crates/okx-host-controller/Cargo.toml'),
  files('crates/okx-research-service/Cargo.toml', 'crates/okx-ipc-contracts/src/lib.rs'),
  files('crates/okx-research-service/Cargo.toml', 'crates/okx-research-service/build.rs'),
  [[{filename: 'crates/okx-research-service/Cargo.toml', status: 'renamed', previous_filename: 'x'}]],
  [[{filename: '../Cargo.toml', status: 'modified'}]],
  [[]], [],
]) assert.throws(() => verify(before, head, diff, 'research'));

const included = structuredClone(head);
included.resolve.nodes.find(n => n.id === 'okx-trading-engine').deps.push(edge('okx-research-service'));
const includedBefore = structuredClone(before);
includedBefore.resolve.nodes.find(n => n.id === 'okx-trading-engine').deps.push(edge('okx-research-service'));
assert.throws(() => verify(includedBefore, included,
  files('crates/okx-research-service/Cargo.toml'), 'research'),
  /EDITED_MANIFEST_IN_PROTECTED_CARGO_CLOSURE/);

console.log('CI_WINDOWS_CARGO_CLOSURE_NEGATIVE_CONTROLS=PASS');
if (process.argv[2]) {
  // The existing dependency-audit already resolved this complete Cargo.lock;
  // also exercise the verifier on real Cargo package/node schema. This does
  // not authorize any scoped CI route or introduce another cargo invocation.
  const actual = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
  for (const [scope, diff] of [
    ['research', files('crates/okx-research-service/Cargo.toml')],
    ['host-controller', files('crates/okx-host-controller/Cargo.toml')],
  ]) {
    assert.equal(verify(actual, actual, diff, scope).protected_closures_identical, true);
  }
  console.log('CI_REAL_CARGO_GRAPH_SCHEMA=PASS');

  // Exercise the *actual resolved workspace graph*, not only a synthetic
  // graph. A manifest may change the owner-specific metadata while leaving
  // every protected consumer closure intact. Conversely, even a declaration
  // change in a shared dependency must force the full financial matrix.
  const declaredFeature = (meta, name) => {
    const owners = meta.packages.filter(pkg => pkg.source == null && pkg.name === name);
    assert.equal(owners.length, 1, `CI_REAL_CARGO_OWNER_MISSING:${name}`);
    assert.ok(owners[0].features && typeof owners[0].features === 'object');
    owners[0].features.ci_560_closure_test = [];
  };
  for (const [scope, owner, diff] of [
    ['research', 'okx-research-service', files('crates/okx-research-service/Cargo.toml')],
    ['host-controller', 'okx-host-controller', files('crates/okx-host-controller/Cargo.toml')],
    ['host-launcher', 'okx-host-launcher', files('crates/okx-host-launcher/Cargo.toml')],
  ]) {
    const ownerOnly = structuredClone(actual);
    declaredFeature(ownerOnly, owner);
    assert.equal(verify(actual, ownerOnly, diff, scope).protected_closures_identical, true,
      `CI_REAL_CARGO_OWNER_SCOPING_FAILED:${scope}`);
  }
  for (const shared of ['okx-trading-engine', 'okx-ipc-contracts']) {
    const changedShared = structuredClone(actual);
    declaredFeature(changedShared, shared);
    for (const [scope, diff] of [
      ['research', files('crates/okx-research-service/Cargo.toml', 'Cargo.lock')],
      ['host-controller', files('crates/okx-host-controller/Cargo.toml', 'Cargo.lock')],
    ]) {
      assert.throws(() => verify(actual, changedShared, diff, scope),
        /PROTECTED_WINDOWS_CARGO_CLOSURE_CHANGED/,
        `CI_REAL_CARGO_SHARED_CHANGE_MUST_RUN_NATIVE:${shared}:${scope}`);
    }
  }
  console.log('CI_REAL_CARGO_CLOSURE_POSITIVE_NEGATIVE_CONTROLS=PASS');
}

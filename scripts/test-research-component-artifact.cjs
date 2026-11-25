'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {sha256, validateManifest, verifyDirectory} = require('./research-component-artifact.cjs');

const head = 'a'.repeat(40), tree = 'b'.repeat(40);
const lock = Buffer.from('version = 4\n'), worker = Buffer.from('synthetic Research Worker bytes');
const manifest = () => ({
  schema: 'okx.windows.research-component/v1', repository_id: 1388071566,
  source_head_sha: head, source_tree: tree,
  rust_version: 'rustc 1.100.0 (fakehash 2026-09-01)',
  cargo_lock_sha256: sha256(lock),
  worker: {sha256: sha256(worker), size: worker.length,
    protocol_version: 1, storage_version: 1},
});
const valid = manifest();
assert.equal(validateManifest(valid, lock, worker, head, tree).research_component_sha256,
  sha256(worker));
for (const version of [2, 3]) {
  const named = manifest();
  named.schema = `okx.windows.research-component/v${version}`;
  named.ci_status_policy = `okx.github.actions.research-ready/v${version}`;
  const proof = validateManifest(named, lock, worker, head, tree);
  assert.equal(proof.ci_status_policy, named.ci_status_policy);
  for (const altered of [
    {...named, ci_status_policy: 'okx.github.actions.research-ready/v1'},
    {...named, ci_status_policy: `okx.github.actions.research-ready/v${5 - version}`},
    {...named, unexpected_engine: true},
    {...named, schema: 'okx.windows.research-component/v1'},
  ]) {
    assert.throws(() => validateManifest(altered, lock, worker, head, tree));
  }
  const missing = structuredClone(named);
  delete missing.ci_status_policy;
  assert.throws(() => validateManifest(missing, lock, worker, head, tree), /MANIFEST_FIELDS/);
}

const invalid = (modify, error) => {
  const candidate = structuredClone(valid);
  modify(candidate);
  assert.throws(() => validateManifest(candidate, lock, worker, head, tree), error);
};
invalid(x => { x.schema = 'okx.windows.bundle/v2'; }, /SCHEMA_UNSUPPORTED/);
invalid(x => { x.ci_status_policy = 'okx.github.actions.research-ready/v3'; }, /MANIFEST_FIELDS/);
invalid(x => { x.repository_id = 1; }, /SOURCE_OR_LOCK/);
invalid(x => { x.source_head_sha = '0'.repeat(40); }, /SOURCE_OR_LOCK/);
invalid(x => { x.source_tree = '0'.repeat(40); }, /SOURCE_OR_LOCK/);
invalid(x => { x.rust_version = 'rustc 1.99.0'; }, /SOURCE_OR_LOCK/);
invalid(x => { x.cargo_lock_sha256 = '0'.repeat(64); }, /SOURCE_OR_LOCK/);
invalid(x => { x.components = {}; }, /MANIFEST_FIELDS/);
invalid(x => { x.worker.other_executable = 'okx-agent.exe'; }, /WORKER_FIELDS/);
invalid(x => { x.worker.sha256 = '0'.repeat(64); }, /BYTES_OR_COMPATIBILITY/);
invalid(x => { x.worker.sha256 = sha256(worker).toUpperCase(); }, /BYTES_OR_COMPATIBILITY/);
invalid(x => { x.worker.size++; }, /BYTES_OR_COMPATIBILITY/);
invalid(x => { x.worker.protocol_version = 2; }, /BYTES_OR_COMPATIBILITY/);
invalid(x => { x.worker.storage_version = 0; }, /BYTES_OR_COMPATIBILITY/);
assert.throws(() => validateManifest(valid, Buffer.from('different lock'), worker, head, tree),
  /SOURCE_OR_LOCK/);
assert.throws(() => validateManifest(valid, lock, Buffer.from('different worker'), head, tree),
  /BYTES_OR_COMPATIBILITY/);
assert.throws(() => validateManifest(valid, lock, worker, head, '0'.repeat(40)),
  /SOURCE_OR_LOCK/);

const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'okx-research-component-'));
const file = name => path.join(directory, name);
const write = () => {
  fs.writeFileSync(file('manifest.json'), JSON.stringify(valid));
  fs.writeFileSync(file('Cargo.lock'), lock);
  fs.writeFileSync(file('okx-research-service.exe'), worker);
};
try {
  write();
  assert.equal(verifyDirectory(directory, head, tree).no_engine_binary_or_dependency_base, true);
  fs.writeFileSync(file('okx-agent.exe'), 'forbidden Engine bytes');
  assert.throws(() => verifyDirectory(directory, head, tree), /EXACT_THREE_FILES/);
  fs.unlinkSync(file('okx-agent.exe'));
  fs.unlinkSync(file('Cargo.lock'));
  assert.throws(() => verifyDirectory(directory, head, tree), /EXACT_THREE_FILES/);
  write();
  fs.writeFileSync(file('okx-research-service.exe'), 'altered');
  assert.throws(() => verifyDirectory(directory, head, tree), /BYTES_OR_COMPATIBILITY/);
  write();
  fs.writeFileSync(file('manifest.json'), JSON.stringify({...valid, extra: 1}));
  assert.throws(() => verifyDirectory(directory, head, tree), /MANIFEST_FIELDS/);
} finally {
  fs.rmSync(directory, {recursive: true, force: true});
}
console.log('RESEARCH_COMPONENT_EXACT_THREE_FILE_NEGATIVE_CONTROLS=PASS');

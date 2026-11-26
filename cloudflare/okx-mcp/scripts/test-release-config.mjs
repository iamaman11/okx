import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { releaseConfig } from './release-config.mjs';

const canonical = JSON.parse(readFileSync('wrangler.jsonc', 'utf8'));
const tree = 'a'.repeat(40);
const researchFingerprint = 'b'.repeat(64);
const original = JSON.stringify(canonical);
const final = releaseConfig(canonical, tree, researchFingerprint);
assert.equal(final.vars.RELEASE_SOURCE_TREE, tree);
assert.equal(final.vars.RUNTIME_TOKEN_SHA256, canonical.vars.RUNTIME_TOKEN_SHA256);
assert.equal(final.vars.RESEARCH_RUNTIME_TOKEN_SHA256, researchFingerprint);
assert.deepEqual(final.kv_namespaces, canonical.kv_namespaces);
assert.deepEqual(final.durable_objects, canonical.durable_objects);
assert.deepEqual(final.migrations, canonical.migrations);
assert.equal(JSON.stringify(canonical), original, 'canonical configuration cannot be mutated');
assert.deepEqual(Object.keys(final.vars).sort(),
  [...Object.keys(canonical.vars), 'RELEASE_SOURCE_TREE', 'RESEARCH_RUNTIME_TOKEN_SHA256'].sort());
assert.throws(() => releaseConfig(canonical, '', researchFingerprint), /invalid release source tree/);
assert.throws(() => releaseConfig(canonical, 'b'.repeat(39), researchFingerprint), /invalid release source tree/);
assert.throws(() => releaseConfig({ ...canonical, vars: {} }, tree, researchFingerprint), /fingerprint/);
assert.throws(() => releaseConfig(canonical, tree), /protected Research runtime fingerprint/);
assert.throws(() => releaseConfig(canonical, tree, ''), /protected Research runtime fingerprint/);
assert.throws(() => releaseConfig(canonical, tree, 'B'.repeat(64)), /protected Research runtime fingerprint/);
assert.throws(() => releaseConfig(canonical, tree, 'b'.repeat(63)), /protected Research runtime fingerprint/);
assert.throws(() => releaseConfig(canonical, tree, canonical.vars.RUNTIME_TOKEN_SHA256), /must be distinct/);
assert.throws(() => releaseConfig({ ...canonical, vars: {
  ...canonical.vars, RELEASE_SOURCE_TREE: tree,
} }, tree, researchFingerprint), /exclusively/);
assert.throws(() => releaseConfig({ ...canonical, vars: {
  ...canonical.vars, RESEARCH_RUNTIME_TOKEN_SHA256: researchFingerprint,
} }, tree, researchFingerprint), /protected deploy environment/);
assert.throws(() => releaseConfig({ ...canonical, kv_namespaces: [] }, tree, researchFingerprint), /bindings/);
assert.throws(() => releaseConfig({ ...canonical, durable_objects: { bindings: [] } }, tree, researchFingerprint), /bindings/);

// The real deploy invokes the CLI, not the exported helper. Prove it fails
// before writing a Wrangler configuration and never prints the fingerprint.
const directory = mkdtempSync(join(tmpdir(), 'okx-edge-release-config-'));
try {
  const output = join(directory, 'wrangler.release.jsonc');
  const missing = spawnSync(process.execPath, ['scripts/release-config.mjs', tree, output], {
    encoding: 'utf8', env: { ...process.env, OKX_RESEARCH_RUNTIME_TOKEN_SHA256: '' },
  });
  assert.notEqual(missing.status, 0, 'missing protected secret must fail closed');
  assert.equal(existsSync(output), false, 'missing secret must not generate a deploy config');

  const generated = spawnSync(process.execPath, ['scripts/release-config.mjs', tree, output], {
    encoding: 'utf8',
    env: { ...process.env, OKX_RESEARCH_RUNTIME_TOKEN_SHA256: researchFingerprint },
  });
  assert.equal(generated.status, 0, generated.stderr);
  assert.doesNotMatch(generated.stdout + generated.stderr,
    new RegExp(researchFingerprint), 'the fingerprint must not enter action logs');
  assert.deepEqual(JSON.parse(readFileSync(output, 'utf8')), final);
  if (process.platform !== 'win32') {
    assert.equal(statSync(output).mode & 0o777, 0o600);
  }
} finally {
  rmSync(directory, { recursive: true, force: true });
}
console.log('worker_release_bindings_positive_negative PASS');

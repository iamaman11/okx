import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { releaseConfig } from './release-config.mjs';

const canonical = JSON.parse(readFileSync('wrangler.jsonc', 'utf8'));
const tree = 'a'.repeat(40);
const original = JSON.stringify(canonical);
const final = releaseConfig(canonical, tree);
assert.equal(final.vars.RELEASE_SOURCE_TREE, tree);
assert.equal(final.vars.RUNTIME_TOKEN_SHA256, canonical.vars.RUNTIME_TOKEN_SHA256);
assert.deepEqual(final.kv_namespaces, canonical.kv_namespaces);
assert.deepEqual(final.durable_objects, canonical.durable_objects);
assert.deepEqual(final.migrations, canonical.migrations);
assert.equal(JSON.stringify(canonical), original, 'canonical configuration cannot be mutated');
assert.deepEqual(Object.keys(final.vars).sort(),
  [...Object.keys(canonical.vars), 'RELEASE_SOURCE_TREE'].sort());
assert.throws(() => releaseConfig(canonical, ''), /invalid release source tree/);
assert.throws(() => releaseConfig(canonical, 'b'.repeat(39)), /invalid release source tree/);
assert.throws(() => releaseConfig({ ...canonical, vars: {} }, tree), /fingerprint/);
assert.throws(() => releaseConfig({ ...canonical, vars: {
  ...canonical.vars, RELEASE_SOURCE_TREE: tree,
} }, tree), /exclusively/);
assert.throws(() => releaseConfig({ ...canonical, kv_namespaces: [] }, tree), /bindings/);
assert.throws(() => releaseConfig({ ...canonical, durable_objects: { bindings: [] } }, tree), /bindings/);
console.log('worker_release_bindings_positive_negative PASS');

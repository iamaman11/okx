import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { releaseConfig } from './release-config.mjs';

const canonical = JSON.parse(readFileSync('wrangler.jsonc', 'utf8'));
const tree = 'a'.repeat(40);
const researchFingerprint = 'b'.repeat(64);
const controlFingerprint = 'c'.repeat(64);
const original = JSON.stringify(canonical);
const deployWorkflow = readFileSync('../../.github/workflows/cloudflare-mcp-deploy.yml', 'utf8');
function section(start, end) {
  const from = deployWorkflow.indexOf(start);
  const to = deployWorkflow.indexOf(end, from + start.length);
  assert.ok(from >= 0 && to > from, 'expected Cloudflare deploy workflow section missing');
  return deployWorkflow.slice(from, to);
}
const preflightSection = section('      - name: Secret preflight\n',
  '      - name: One-time v2 installation input preflight\n');
const deploySection = section('      - name: Deploy Worker\n',
  '      - name: Initialize isolated clean-v2 governance once\n');
const protectedBinding = 'OKX_CONTROL_ATTEST_TOKEN_SHA256: ${{ secrets.OKX_CONTROL_ATTEST_TOKEN_SHA256 }}';
assert.ok(preflightSection.includes(protectedBinding), 'protected CONTROL secret must reach deploy preflight');
assert.ok(deploySection.includes(protectedBinding), 'protected CONTROL secret must reach release-config CLI');
assert.match(preflightSection, /if test -n "\$OKX_CONTROL_ATTEST_TOKEN_SHA256"; then/);
assert.match(preflightSection, /"\$OKX_CONTROL_ATTEST_TOKEN_SHA256" = "\$OKX_RESEARCH_RUNTIME_TOKEN_SHA256"/);
const cleanup = deploySection.indexOf("trap 'rm -f wrangler.release.jsonc' EXIT");
const create = deploySection.indexOf('node scripts/release-config.mjs');
assert.ok(cleanup >= 0 && create > cleanup,
  'release config cleanup must be installed before any fingerprint can be written');
const final = releaseConfig(canonical, tree, researchFingerprint);
assert.equal(final.vars.RELEASE_SOURCE_TREE, tree);
assert.equal(final.vars.RUNTIME_TOKEN_SHA256, canonical.vars.RUNTIME_TOKEN_SHA256);
assert.equal(final.vars.RESEARCH_RUNTIME_TOKEN_SHA256, researchFingerprint);
assert.deepEqual(final.kv_namespaces, canonical.kv_namespaces);
assert.deepEqual(final.durable_objects, canonical.durable_objects);
assert.deepEqual(final.migrations, canonical.migrations);
assert.equal(JSON.stringify(canonical), original, 'canonical configuration cannot be mutated');
assert.deepEqual(Object.keys(final.vars).sort(),
  [...Object.keys(canonical.vars), 'RELEASE_SOURCE_TREE', 'RESEARCH_RUNTIME_TOKEN_SHA256',
    'CONTROL_ATTEST_TOKEN_SHA256'].sort());
assert.equal(final.vars.CONTROL_ATTEST_TOKEN_SHA256, 'disabled',
  'missing optional secret must override a stale Worker secret with invalid auth fingerprint');
assert.deepEqual(releaseConfig(canonical, tree, researchFingerprint, ''), final,
  'empty GitHub Actions secret must explicitly disable CONTROL attestation');
const enabled = releaseConfig(canonical, tree, researchFingerprint, controlFingerprint);
assert.deepEqual(enabled.vars, { ...final.vars, CONTROL_ATTEST_TOKEN_SHA256: controlFingerprint });
assert.deepEqual(enabled.kv_namespaces, canonical.kv_namespaces);
assert.deepEqual(enabled.durable_objects, canonical.durable_objects);
assert.throws(() => releaseConfig(canonical, '', researchFingerprint), /invalid release source tree/);
assert.throws(() => releaseConfig(canonical, 'b'.repeat(39), researchFingerprint), /invalid release source tree/);
assert.throws(() => releaseConfig({ ...canonical, vars: {} }, tree, researchFingerprint), /fingerprint/);
assert.throws(() => releaseConfig(canonical, tree), /protected Research runtime fingerprint/);
assert.throws(() => releaseConfig(canonical, tree, ''), /protected Research runtime fingerprint/);
assert.throws(() => releaseConfig(canonical, tree, 'B'.repeat(64)), /protected Research runtime fingerprint/);
assert.throws(() => releaseConfig(canonical, tree, 'b'.repeat(63)), /protected Research runtime fingerprint/);
assert.throws(() => releaseConfig(canonical, tree, canonical.vars.RUNTIME_TOKEN_SHA256), /must be distinct/);
for (const invalid of ['C'.repeat(64), 'c'.repeat(63), 'c'.repeat(65), 'c'.repeat(63) + '\n', null, true]) {
  assert.throws(() => releaseConfig(canonical, tree, researchFingerprint, invalid), /CONTROL attestor fingerprint invalid/);
}
assert.throws(() => releaseConfig(canonical, tree, researchFingerprint, researchFingerprint), /must be distinct/);
assert.throws(() => releaseConfig(canonical, tree, researchFingerprint, canonical.vars.RUNTIME_TOKEN_SHA256), /must be distinct/);
assert.throws(() => releaseConfig({ ...canonical, vars: {
  ...canonical.vars, RELEASE_SOURCE_TREE: tree,
} }, tree, researchFingerprint), /exclusively/);
assert.throws(() => releaseConfig({ ...canonical, vars: {
  ...canonical.vars, RESEARCH_RUNTIME_TOKEN_SHA256: researchFingerprint,
} }, tree, researchFingerprint), /protected deploy environment/);
assert.throws(() => releaseConfig({ ...canonical, vars: {
  ...canonical.vars, CONTROL_ATTEST_TOKEN_SHA256: controlFingerprint,
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
    env: { ...process.env, OKX_RESEARCH_RUNTIME_TOKEN_SHA256: researchFingerprint,
      OKX_CONTROL_ATTEST_TOKEN_SHA256: '' },
  });
  assert.equal(generated.status, 0, generated.stderr);
  assert.doesNotMatch(generated.stdout + generated.stderr,
    new RegExp(researchFingerprint), 'the fingerprint must not enter action logs');
  assert.deepEqual(JSON.parse(readFileSync(output, 'utf8')), final);
  if (process.platform !== 'win32') {
    assert.equal(statSync(output).mode & 0o777, 0o600);
  }

  const enabledOutput = join(directory, 'wrangler.enabled.jsonc');
  const configured = spawnSync(process.execPath, ['scripts/release-config.mjs', tree, enabledOutput], {
    encoding: 'utf8',
    env: { ...process.env, OKX_RESEARCH_RUNTIME_TOKEN_SHA256: researchFingerprint,
      OKX_CONTROL_ATTEST_TOKEN_SHA256: controlFingerprint },
  });
  assert.equal(configured.status, 0, configured.stderr);
  assert.doesNotMatch(configured.stdout + configured.stderr, new RegExp(controlFingerprint),
    'CONTROL fingerprint must not enter action logs');
  assert.deepEqual(JSON.parse(readFileSync(enabledOutput, 'utf8')), enabled);

  const rejectedOutput = join(directory, 'wrangler.rejected.jsonc');
  const rejected = spawnSync(process.execPath, ['scripts/release-config.mjs', tree, rejectedOutput], {
    encoding: 'utf8',
    env: { ...process.env, OKX_RESEARCH_RUNTIME_TOKEN_SHA256: researchFingerprint,
      OKX_CONTROL_ATTEST_TOKEN_SHA256: researchFingerprint },
  });
  assert.notEqual(rejected.status, 0, 'colliding CONTROL secret must fail closed');
  assert.equal(existsSync(rejectedOutput), false, 'invalid CONTROL secret must not generate deploy config');
  assert.doesNotMatch(rejected.stdout + rejected.stderr, new RegExp(researchFingerprint),
    'rejected fingerprint must not enter error logs');
} finally {
  rmSync(directory, { recursive: true, force: true });
}
console.log('worker_release_bindings_positive_negative PASS');

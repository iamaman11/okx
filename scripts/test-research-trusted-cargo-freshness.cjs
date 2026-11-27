#!/usr/bin/env node
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {execFileSync} = require('node:child_process');
const {reconcile, CRATES} = require('./research-trusted-cargo-freshness.cjs');

const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'okx-trusted-cargo-freshness-'));
const git = (...args) => execFileSync('git', args, {
  cwd: temp, encoding: 'utf8', timeout: 10000, windowsHide: true,
}).trim();
const write = (relative, content) => {
  const file = path.join(temp, ...relative.split('/'));
  fs.mkdirSync(path.dirname(file), {recursive: true});
  fs.writeFileSync(file, content);
  return file;
};
const sha = () => git('rev-parse', 'HEAD');
const run = (seed, head, options = {}) => reconcile({
  root: temp, seed, expectedHead: head, profile: 'release',
  exactCacheHit: 'true', researchOnly: 'true', fetchMissingSeed: false, ...options,
});
try {
  git('init', '-q');
  git('config', 'user.email', 'ci-fixture@example.invalid');
  git('config', 'user.name', 'CI fixture');
  write('Cargo.toml', '[workspace]\nmembers = []\n');
  write('Cargo.lock', '# fixture\n');
  for (const name of CRATES) {
    write(`crates/${name}/Cargo.toml`, `[package]\nname = "${name}"\nversion = "0.1.0"\n`);
    write(`crates/${name}/src/lib.rs`, `pub fn ${name.replaceAll('-', '_')}() {}\n`);
  }
  git('add', '.');
  git('commit', '-qm', 'trusted seed content');
  const seed = sha();

  const changed = write(
    'crates/okx-research-service/src/analytics/tests/jobs.rs',
    '#[test] fn new_diagnostic_case() {}\n',
  );
  git('add', '.');
  git('commit', '-qm', 'real research-only test change');
  const head = sha();
  const anchor = Date.now() - 90_000;
  const candidate = path.join(temp, 'crates', 'okx-research-domain', 'src', 'lib.rs');
  for (const name of CRATES) {
    const depInfo = write(
      `target/release/build/${name}/test-unit/fingerprint/dep-lib-${name}`,
      'cargo unit',
    );
    fs.utimesSync(depInfo, anchor / 1000, anchor / 1000);
  }
  fs.utimesSync(candidate, Date.now() / 1000, Date.now() / 1000);
  fs.utimesSync(changed, Date.now() / 1000, Date.now() / 1000);
  // A second, newer Cargo unit must veto an unsafe changed source timestamp.
  const newestServiceDep = write(
    'target/release/build/okx-research-service/second-unit/fingerprint/dep-bin-service',
    'newer Cargo unit',
  );
  fs.utimesSync(newestServiceDep, (anchor + 40_000) / 1000, (anchor + 40_000) / 1000);
  const untouchedBefore = fs.statSync(candidate).mtimeMs;

  assert.equal(run(seed, head, {exactCacheHit: 'false'}).reason,
    'NO_EXACT_TRUSTED_CARGO_CACHE_HIT');
  assert.equal(fs.statSync(candidate).mtimeMs, untouchedBefore);
  assert.equal(run(seed, head, {researchOnly: 'false'}).reason, 'NOT_RESEARCH_ONLY');
  assert.equal(fs.statSync(candidate).mtimeMs, untouchedBefore);
  assert.throws(() => run(seed, 'f'.repeat(40)), /RESEARCH_CACHE_WRONG_PR_HEAD/);
  assert.equal(fs.statSync(candidate).mtimeMs, untouchedBefore);

  fs.utimesSync(changed, (anchor + 20_000) / 1000, (anchor + 20_000) / 1000);
  assert.throws(() => run(seed, head), /RESEARCH_CACHE_CHANGED_SOURCE_TIME_UNSAFE/);
  assert.equal(fs.statSync(candidate).mtimeMs, untouchedBefore,
    'unsafe changed source must prevent every timestamp mutation');
  fs.utimesSync(changed, Date.now() / 1000, Date.now() / 1000);
  const safeChangedBefore = fs.statSync(changed).mtimeMs;

  const pass = run(seed, head);
  assert.equal(pass.state, 'NORMALIZED');
  assert.equal(pass.changed, 1);
  assert.equal(pass.noAnchor.length, 0);
  assert.equal(pass.normalized, CRATES.length);
  assert.ok(fs.statSync(candidate).mtimeMs < anchor);
  assert.equal(fs.statSync(changed).mtimeMs, safeChangedBefore,
    'changed Research test source must stay newer than cached dep-info');

  // Current on-disk bytes, not merely a claimed Git tree, must match.
  fs.writeFileSync(candidate, 'pub fn tampered() {}\n');
  assert.equal(run(seed, head).reason, 'DIRTY_CHECKOUT');
  git('restore', '--', 'crates/okx-research-domain/src/lib.rs');

  // Changes to the build configuration prohibit timestamp normalization.
  write('Cargo.lock', '# changed fixture\n');
  git('add', 'Cargo.lock');
  git('commit', '-qm', 'changed build inputs');
  const changedManifestHead = sha();
  fs.utimesSync(candidate, Date.now() / 1000, Date.now() / 1000);
  const beforeManifestAttempt = fs.statSync(candidate).mtimeMs;
  assert.equal(run(seed, changedManifestHead).reason, 'CARGO_INPUTS_DIFFER_FROM_SEED');
  assert.equal(fs.statSync(candidate).mtimeMs, beforeManifestAttempt);

  assert.equal(run('0'.repeat(40), changedManifestHead).reason, 'SEED_COMMIT_MISSING');
  process.stdout.write('trusted Research Cargo freshness positive and negative fixtures PASS\n');
} finally {
  // Only this isolated, freshly-created fixture root is removed.
  fs.rmSync(temp, {recursive: true, force: true});
}

#!/usr/bin/env node
// Reconcile checkout mtimes with an immutable, trusted-main Cargo cache.
// Cargo 1.100 compares source mtimes to cached dep-info and otherwise rebuilds
// unchanged workspace dependencies after every fresh GitHub checkout.
//
// This is deliberately NOT a generic restore-mtime script: only exact Git
// blobs present in the known successful main seed may be retimed. Windows
// checkout CRLF is accepted only when removing CR before LF reproduces that
// same exact Git blob. Everything else fails closed.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const {execFileSync} = require('node:child_process');

const TRUSTED_MAIN_SEED = 'c95acdbd9fe426f064414661293452e48d2690e1';
const CRATES = Object.freeze([
  'okx-strategy-core', 'okx-research', 'okx-research-service',
  'okx-analysis', 'okx-research-domain', 'okx-research-protocol',
]);
const SHA = /^[a-f0-9]{40}$/;

function git(root, args) {
  return execFileSync('git', args, {
    cwd: root, maxBuffer: 8 * 1024 * 1024, timeout: 30000,
    windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'],
  });
}
function commitExists(root, sha) {
  try { git(root, ['cat-file', '-e', `${sha}^{commit}`]); return true; }
  catch { return false; }
}
function sameManifests(root, seed) {
  // A Cargo profile, lock, config or build-script change invalidates this
  // optimization, regardless of the cache action's reported hit.
  const tracked = [
    'Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', '.cargo', '.gitattributes',
    ':(glob)crates/**/.gitattributes',
    ':(glob)crates/**/Cargo.toml', ':(glob)crates/**/build.rs',
  ];
  try { git(root, ['diff', '--quiet', seed, 'HEAD', '--', ...tracked]); return true; }
  catch { return false; }
}
function sourceTree(root, ref) {
  const paths = CRATES.map(name => `crates/${name}/src`);
  const output = git(root, ['ls-tree', '-r', '-z', '--full-tree', ref, '--', ...paths]);
  const map = new Map();
  for (const entry of output.toString('utf8').split('\0')) {
    if (!entry) continue;
    const hit = /^(\d+) blob ([a-f0-9]{40})\t(.+)$/.exec(entry);
    if (!hit) throw Error('RESEARCH_CACHE_UNEXPECTED_TREE_ENTRY');
    if (hit[3].endsWith('.rs')) map.set(hit[3], {mode: hit[1], sha: hit[2]});
  }
  return map;
}
function gitBlobHash(bytes) {
  return crypto.createHash('sha1')
    .update(`blob ${bytes.length}\0`).update(bytes).digest('hex');
}
function checkoutBlobMatches(bytes, expected) {
  if (gitBlobHash(bytes) === expected) return 'EXACT';
  // The hosted Windows checkout can materialize a tracked LF Git blob as
  // CRLF even with a clean Git index. Admit only this one byte-level checkout
  // transformation; a source edit or any other filter output stays unsafe.
  const normalized = Buffer.allocUnsafe(bytes.length);
  let written = 0;
  let crlfPairs = 0;
  for (let i = 0; i < bytes.length; i++) {
    if (bytes[i] === 13 && i + 1 < bytes.length && bytes[i + 1] === 10) {
      crlfPairs++;
      continue;
    }
    normalized[written++] = bytes[i];
  }
  if (crlfPairs && gitBlobHash(normalized.subarray(0, written)) === expected) {
    return 'CHECKOUT_CRLF';
  }
  return null;
}
function depInfoAnchor(root, profile, name) {
  // Cargo 1.100's verified Windows layout for this repository. Never use the
  // historical target/<profile>/.fingerprint path as a silent substitute.
  const pkg = path.join(root, 'target', profile, 'build', name);
  if (!fs.existsSync(pkg)) return null;
  const stamps = [];
  for (const unit of fs.readdirSync(pkg, {withFileTypes: true})) {
    if (!unit.isDirectory()) continue;
    const folder = path.join(pkg, unit.name, 'fingerprint');
    if (!fs.existsSync(folder)) continue;
    for (const name of fs.readdirSync(folder)) {
      if (!name.startsWith('dep-')) continue;
      const item = path.join(folder, name);
      if (fs.statSync(item).isFile()) stamps.push(fs.statSync(item).mtimeMs);
    }
  }
  return stamps.length ? {
    beforeOldest: Math.min(...stamps) - 2000,
    newest: Math.max(...stamps),
  } : null;
}
function inspect(root, seed, profile) {
  const baseline = sourceTree(root, seed);
  const current = sourceTree(root, 'HEAD');
  const changes = [];
  const retime = [];
  const noAnchor = [];
  let checkoutCrlf = 0;
  for (const name of CRATES) {
    const prefix = `crates/${name}/src/`;
    const paths = [...current.keys()].filter(value => value.startsWith(prefix));
    const anchor = depInfoAnchor(root, profile, name);
    if (anchor === null || !Number.isFinite(anchor.beforeOldest) ||
        !Number.isFinite(anchor.newest) || anchor.beforeOldest <= 0) {
      noAnchor.push(name);
      continue;
    }
    for (const relative of paths) {
      const info = current.get(relative);
      const abs = path.join(root, ...relative.split('/'));
      const file = fs.lstatSync(abs);
      if (!file.isFile() || file.isSymbolicLink() || info.mode !== '100644') {
        throw Error('RESEARCH_CACHE_NONREGULAR_SOURCE');
      }
      const match = checkoutBlobMatches(fs.readFileSync(abs), info.sha);
      if (!match) {
        throw Error('RESEARCH_CACHE_WORKTREE_MISMATCH');
      }
      if (match === 'CHECKOUT_CRLF') checkoutCrlf++;
      const seeded = baseline.get(relative);
      if (!seeded || seeded.sha !== info.sha || seeded.mode !== info.mode) {
        changes.push(relative);
        // Checkout must make changed files newer than every restored
        // fingerprint. Never move a changed file backward in time.
        if (file.mtimeMs <= anchor.newest) {
          throw Error('RESEARCH_CACHE_CHANGED_SOURCE_TIME_UNSAFE');
        }
        continue;
      }
      if (file.mtimeMs >= anchor.beforeOldest) {
        retime.push({abs, atime: file.atime, anchor: anchor.beforeOldest});
      }
    }
  }
  return {retime, changes, noAnchor, checkoutCrlf};
}
function reconcile({
  root, profile, expectedHead, exactCacheHit, researchOnly,
  seed = TRUSTED_MAIN_SEED, fetchMissingSeed = true,
}) {
  if (!['debug', 'release'].includes(profile)) throw Error('RESEARCH_CACHE_INVALID_PROFILE');
  if (!SHA.test(expectedHead || '') || !SHA.test(seed)) throw Error('RESEARCH_CACHE_INVALID_COMMIT');
  if (git(root, ['rev-parse', 'HEAD']).toString().trim() !== expectedHead) {
    throw Error('RESEARCH_CACHE_WRONG_PR_HEAD');
  }
  const outcome = {state: 'SKIPPED', reason: null, normalized: 0, changed: 0, noAnchor: []};
  function skip(reason) { outcome.reason = reason; return outcome; }
  if (researchOnly !== 'true') return skip('NOT_RESEARCH_ONLY');
  if (exactCacheHit !== 'true') return skip('NO_EXACT_TRUSTED_CARGO_CACHE_HIT');
  if (git(root, ['status', '--porcelain', '--untracked-files=no']).toString().trim()) {
    return skip('DIRTY_CHECKOUT');
  }
  if (!commitExists(root, seed)) {
    if (!fetchMissingSeed) return skip('SEED_COMMIT_MISSING');
    try {
      git(root, ['fetch', '--no-tags', '--depth=1', 'origin', seed]);
    } catch { return skip('SEED_FETCH_UNAVAILABLE'); }
  }
  if (!commitExists(root, seed)) return skip('SEED_COMMIT_UNAVAILABLE');
  if (!sameManifests(root, seed)) return skip('CARGO_INPUTS_DIFFER_FROM_SEED');
  const {retime, changes, noAnchor, checkoutCrlf} = inspect(root, seed, profile);
  // Full preflight is complete before the first write. A failure while
  // updating a checkout timestamp must fail CI rather than mask stale units.
  for (const item of retime) {
    fs.utimesSync(item.abs, item.atime, new Date(item.anchor));
    if (fs.statSync(item.abs).mtimeMs >= item.anchor + 1500) {
      throw Error('RESEARCH_CACHE_TIMESTAMP_WRITE_UNVERIFIED');
    }
  }
  return {state: 'NORMALIZED', reason: null, normalized: retime.length,
    changed: changes.length, noAnchor, checkoutCrlf};
}
function main() {
  const args = Object.fromEntries(process.argv.slice(2).map(arg => {
    const index = arg.indexOf('=');
    if (index < 1) throw Error('RESEARCH_CACHE_INVALID_ARGUMENT');
    return [arg.slice(0, index), arg.slice(index + 1)];
  }));
  if (!process.env.GITHUB_WORKSPACE) throw Error('RESEARCH_CACHE_NO_WORKSPACE');
  const outcome = reconcile({
    root: fs.realpathSync(process.env.GITHUB_WORKSPACE),
    profile: args.profile,
    expectedHead: args.head,
    exactCacheHit: args.cacheHit,
    researchOnly: args.researchOnly,
  });
  const summary = `Research trusted Cargo freshness: ${outcome.state} ${outcome.reason || 'EXACT_SEED_CONTENT'}, normalized=${outcome.normalized}, changed=${outcome.changed}, checkout_crlf=${outcome.checkoutCrlf || 0}, missing_anchor=${outcome.noAnchor.length}`;
  process.stdout.write(summary + '\n');
  if (process.env.GITHUB_STEP_SUMMARY) {
    fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, summary + '\n');
  }
}
if (require.main === module) {
  try { main(); } catch (error) {
    process.stderr.write(`Research trusted Cargo freshness refused: ${error.message}\n`);
    process.exitCode = 1;
  }
}
module.exports = {reconcile, inspect, gitBlobHash, checkoutBlobMatches, TRUSTED_MAIN_SEED, CRATES};

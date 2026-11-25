'use strict';

// #560: a scoped Cargo manifest/lock change may skip Native only when both
// pinned Windows-target resolvers have identical *complete protected* closures.
// This proof is intentionally stricter than Cargo's release-only consumer map:
// dev edges, enabled features and dependency aliases also protect Rust tests.
const fs = require('node:fs');
const path = require('node:path');
const {changedFiles} = require('./ci-component-impact.cjs');

const TARGET = 'x86_64-pc-windows-msvc';
const RESEARCH = new Set([
  'crates/okx-research/Cargo.toml',
  'crates/okx-analysis/Cargo.toml',
  'crates/okx-research-service/Cargo.toml',
  'crates/okx-strategy-core/Cargo.toml',
  'crates/okx-research-protocol/Cargo.toml',
  'crates/okx-research-domain/Cargo.toml',
]);
const HOST = new Set([
  'crates/okx-host-controller/Cargo.toml',
  'crates/okx-host-launcher/Cargo.toml',
]);
const MIXED = new Set([...RESEARCH, ...HOST]);
const PROTECTED = Object.freeze({
  // Bridge is a live MCP consumer of shared IPC contracts. A future dev-only
  // Research protocol edge in Bridge must block a scoped wire/domain edit just
  // as an Engine test dependency does, even though Bridge ships separately.
  research: ['okx-trading-engine', 'okx-host-controller', 'okx-host-launcher', 'okx-bridge-mcp'],
  'host-controller': ['okx-trading-engine', 'okx-research-service'],
  'host-launcher': ['okx-trading-engine', 'okx-research-service'],
  // CONTROL and Research are both freshly tested by the mixed-owner route.
  // Only the financial owner is reused from the immutable accepted release.
  'research-host': ['okx-trading-engine'],
});

const safePath = p => typeof p === 'string' && p.length > 0 && p.length <= 512 &&
  !p.includes('\\') && p.split('/').every(part => /^[A-Za-z0-9_.-]+$/.test(part) && part !== '.' && part !== '..');
const docs = p => p === 'README.md' || (p.startsWith('docs/') && p.endsWith('.md'));
const research = p => RESEARCH.has(p) || ['crates/okx-research/', 'crates/okx-analysis/',
  'crates/okx-research-service/', 'crates/okx-strategy-core/',
  'crates/okx-research-protocol/', 'crates/okx-research-domain/'].some(prefix =>
  (p.startsWith(prefix + 'src/') || p.startsWith(prefix + 'tests/')) && p.endsWith('.rs'));
const protectedResearchSource = p => ['crates/okx-research-protocol/', 'crates/okx-research-domain/']
  .some(prefix => (p.startsWith(prefix + 'src/') || p.startsWith(prefix + 'tests/')) && p.endsWith('.rs'));
const host = p => ['crates/okx-host-controller/src/', 'crates/okx-host-launcher/src/',
  'crates/okx-host-protocol/src/', 'crates/okx-host-controller/tests/',
  'crates/okx-host-launcher/tests/', 'crates/okx-host-protocol/tests/']
  .some(prefix => p.startsWith(prefix)) && p.endsWith('.rs');
const financialRuntimeBoundary = p => [
  'crates/okx-research-service/src/main.rs',
  'crates/okx-research-service/src/process_memory.rs',
  'crates/okx-host-controller/src/job.rs',
  'crates/okx-host-controller/src/lifecycle_executor.rs',
].includes(p);

function candidateFiles(input, scope) {
  if (!Object.hasOwn(PROTECTED, scope)) throw Error('UNSUPPORTED_CARGO_SCOPE');
  // Keep the GitHub diff's status and previous_filename evidence all the way
  // to this authorization boundary. A bare filename list cannot prove that
  // a path wasn't renamed across owners or deleted in the checked commit.
  const files = changedFiles(input).files;
  if (!files || !files.length || files.length > 500 || new Set(files).size !== files.length ||
      !files.every(safePath)) throw Error('CARGO_DIFF_NOT_VERIFIED');
  const manifests = scope === 'research' ? RESEARCH : scope === 'research-host' ? MIXED : HOST;
  const statuses = new Map(input.flat().map(entry => [entry.filename, entry.status]));
  if (files.some(file => (file === 'Cargo.lock' || manifests.has(file) ||
      (['research', 'research-host'].includes(scope) && protectedResearchSource(file))) && statuses.get(file) !== 'modified'))
    throw Error('CARGO_PROOF_REQUIRES_EXISTING_MODIFIED_OWNER_INPUT');
  // Research wire/domain crates may enter a protected owner via future
  // dev/test edges, even if the current Engine build is decoupled. Their
  // source changes need the same *resolved* protected-closure proof as a
  // manifest change. Ordinary owner source files retain their fast route.
  if (!files.some(file => manifests.has(file) ||
      (['research', 'research-host'].includes(scope) && protectedResearchSource(file))))
    throw Error('CARGO_PROOF_CANDIDATE_MISSING');
  if (scope === 'research-host' && (files.some(financialRuntimeBoundary) ||
      !files.some(file => research(file)) || !files.some(file => host(file) || HOST.has(file))))
    throw Error('MIXED_OWNER_FINANCIAL_BOUNDARY_OR_OWNER_MISSING');
  if (!files.every(file => {
    if (file === 'Cargo.lock' || docs(file)) return true;
    if (scope === 'research') return research(file) || file.startsWith('cloudflare/okx-mcp/');
    if (scope === 'research-host') return research(file) || host(file) ||
      HOST.has(file) || file.startsWith('cloudflare/okx-mcp/');
    return host(file) || HOST.has(file) || file.startsWith('cloudflare/okx-mcp/');
  })) throw Error('MIXED_OR_UNSAFE_CARGO_DIFF');
  // A second owner's Cargo.toml can change the Rust feature graph even when
  // a filtered resolver misses an optional target. Such PRs always run full.
  if (files.some(file => file.endsWith('/Cargo.toml') && !manifests.has(file)))
    throw Error('MIXED_CARGO_MANIFEST_OWNERS');
  return files;
}

function normalizedCargo(metadata) {
  if (!metadata || !path.isAbsolute(metadata.workspace_root) ||
      !Array.isArray(metadata.packages) || !Array.isArray(metadata.workspace_members) ||
      !metadata.resolve || !Array.isArray(metadata.resolve.nodes))
    throw Error('WINDOWS_CARGO_METADATA_RESOLUTION_REQUIRED');
  const members = new Set(metadata.workspace_members);
  if (members.size !== metadata.workspace_members.length) throw Error('DUPLICATE_WORKSPACE_MEMBER');
  const packages = new Map(), keys = new Map(), byKey = new Map();
  for (const pkg of metadata.packages) {
    if (!pkg || typeof pkg.id !== 'string' || packages.has(pkg.id) ||
        typeof pkg.name !== 'string' || typeof pkg.version !== 'string' ||
        typeof pkg.manifest_path !== 'string') throw Error('INVALID_CARGO_PACKAGE');
    let key;
    if (pkg.source == null) {
      if (!members.has(pkg.id)) throw Error('EXTERNAL_PATH_DEPENDENCY_UNVERIFIED');
      const relative = path.relative(metadata.workspace_root, pkg.manifest_path).split(path.sep).join('/');
      if (!safePath(relative) || !relative.startsWith('crates/') || !relative.endsWith('/Cargo.toml'))
        throw Error('UNSAFE_WORKSPACE_CARGO_PATH');
      key = `workspace:${relative}:${pkg.name}@${pkg.version}`;
    } else {
      if (typeof pkg.source !== 'string' || !pkg.source ||
          (pkg.checksum != null && typeof pkg.checksum !== 'string'))
        throw Error('UNVERIFIED_EXTERNAL_PACKAGE_SOURCE');
      key = `external:${pkg.name}@${pkg.version}:${pkg.source}:${pkg.checksum ?? ''}`;
    }
    if (byKey.has(key)) throw Error('DUPLICATE_RESOLVED_PACKAGE');
    packages.set(pkg.id, pkg); keys.set(pkg.id, key); byKey.set(key, pkg.id);
  }
  const nodes = new Map(), identities = new Map();
  for (const node of metadata.resolve.nodes) {
    if (!node || typeof node.id !== 'string' || nodes.has(node.id) ||
        !packages.has(node.id) || !Array.isArray(node.deps) || !Array.isArray(node.features))
      throw Error('INCOMPLETE_CARGO_RESOLUTION');
    nodes.set(node.id, node);
  }
  for (const [id, node] of nodes) {
    const pkg = packages.get(id);
    if (!pkg.features || typeof pkg.features !== 'object' || !Array.isArray(pkg.targets))
      throw Error('INCOMPLETE_CARGO_PACKAGE_DETAILS');
    const dependencies = node.deps.map(dep => {
      if (!dep || !keys.has(dep.pkg) || typeof dep.name !== 'string' ||
          !Array.isArray(dep.dep_kinds) || !dep.dep_kinds.length)
        throw Error('UNVERIFIED_CARGO_DEPENDENCY_EDGE');
      const kinds = dep.dep_kinds.map(kind => {
        if (!kind || ![null, 'normal', 'build', 'dev'].includes(kind.kind) ||
            !(kind.target == null || typeof kind.target === 'string'))
          throw Error('UNVERIFIED_CARGO_DEPENDENCY_TARGET');
        return `${kind.kind ?? 'normal'}:${kind.target ?? 'all'}`;
      }).sort();
      return `${dep.name}:${keys.get(dep.pkg)}:${kinds.join(',')}`;
    }).sort();
    const features = Object.entries(pkg.features).map(([name, values]) => {
      if (!Array.isArray(values) || !values.every(value => typeof value === 'string'))
        throw Error('UNVERIFIED_CARGO_FEATURE_DECLARATION');
      return `${name}:${[...values].sort().join(',')}`;
    }).sort();
    const targets = pkg.targets.map(target => {
      if (!target || typeof target.name !== 'string' || !Array.isArray(target.kind) ||
          !Array.isArray(target.crate_types)) throw Error('UNVERIFIED_CARGO_TARGET');
      const required = target['required-features'] ?? target.required_features ?? [];
      if (!Array.isArray(required)) throw Error('UNVERIFIED_REQUIRED_CARGO_FEATURES');
      // Cargo target source paths are part of the protected build graph. A
      // manifest may redirect a bin/lib/test to an unrelated owner's source
      // without changing package IDs, features or dependency edges. Normalize
      // against each package root so detached base/head worktrees compare.
      if (typeof target.src_path !== 'string' || !path.isAbsolute(target.src_path))
        throw Error('UNVERIFIED_CARGO_TARGET_SOURCE');
      const source = path.relative(path.dirname(pkg.manifest_path), target.src_path)
        .split(path.sep).join('/');
      if (!safePath(source)) throw Error('TARGET_SOURCE_OUTSIDE_CARGO_PACKAGE');
      return `${target.name}:${[...target.kind].sort().join(',')}:` +
        `${[...target.crate_types].sort().join(',')}:${target.edition ?? ''}:` +
        `${[...required].sort().join(',')}:${source}`;
    }).sort();
    identities.set(keys.get(id), JSON.stringify({
      features: [...node.features].sort(), dependencies, declared_features: features, targets,
      edition: pkg.edition ?? '', rust_version: pkg.rust_version ?? null, links: pkg.links ?? null,
    }));
  }
  function closure(name) {
    const roots = [...members].filter(id => packages.get(id)?.name === name);
    if (roots.length !== 1) throw Error(`PROTECTED_CARGO_ROOT_MISSING:${name}`);
    const seen = new Map(), pending = [roots[0]];
    while (pending.length) {
      const id = pending.pop(), key = keys.get(id);
      if (seen.has(key)) continue;
      seen.set(key, identities.get(key));
      for (const dep of nodes.get(id).deps) pending.push(dep.pkg);
    }
    return seen;
  }
  return {closure};
}

function verify(baseMetadata, headMetadata, changed, scope) {
  const files = candidateFiles(changed, scope);
  const protectedPaths = compareProtected(baseMetadata, headMetadata, PROTECTED[scope]);
  // Equal package graphs are insufficient for a source-only change: editing
  // code inside a protected dev/build/normal closure still recompiles and can
  // change its test semantics. Reject *every* such source path, not only tomls.
  if (files.some(file => protectedPaths.some(dir => file.startsWith(dir))))
    throw Error('EDITED_SOURCE_IN_PROTECTED_CARGO_CLOSURE');
  return {
    schema: 'okx.560.windows-cargo-closure-proof/v1',
    scope, target: TARGET,
    protected_roots: PROTECTED[scope], changed_files: files.length,
    protected_closures_identical: true,
    executed_financial_tests: false,
  };
}

function compareProtected(baseMetadata, headMetadata, roots) {
  const before = normalizedCargo(baseMetadata), after = normalizedCargo(headMetadata);
  const protectedPaths = new Set();
  for (const name of roots) {
    const left = before.closure(name), right = after.closure(name);
    for (const key of [...left.keys(), ...right.keys()]) {
      if (key.startsWith('workspace:')) protectedPaths.add(path.posix.dirname(key.split(':')[1]) + '/');
      if (!left.has(key) || !right.has(key) || left.get(key) !== right.get(key))
        throw Error(`PROTECTED_WINDOWS_CARGO_CLOSURE_CHANGED:${name}:${key}`);
    }
  }
  return [...protectedPaths].sort();
}

function verifyEngineInputs(baseMetadata, headMetadata, files) {
  const protectedPaths = compareProtected(baseMetadata, headMetadata, ['okx-trading-engine']);
  if (files.some(file => protectedPaths.some(dir => file.startsWith(dir))))
    throw Error('EDITED_SOURCE_IN_ENGINE_WINDOWS_CARGO_CLOSURE');
  return protectedPaths;
}

function verifyUnchangedOwnerInputs(baseMetadata, headMetadata, files, roots) {
  const protectedPaths = compareProtected(baseMetadata, headMetadata, roots);
  if (files.some(file => protectedPaths.some(dir => file.startsWith(dir))))
    throw Error('EDITED_SOURCE_IN_PROTECTED_WINDOWS_CARGO_CLOSURE');
  return protectedPaths;
}

if (require.main === module) {
  try {
    if (process.argv.length !== 6) throw Error('USAGE: ci-cargo-closure-proof <base-meta> <head-meta> <diff-json> <research|host-controller|host-launcher>');
    const args = process.argv.slice(2, 5).map(file => JSON.parse(fs.readFileSync(file, 'utf8')));
    process.stdout.write(JSON.stringify(verify(...args, process.argv[5])) + '\n');
  } catch (error) {
    console.error(`WINDOWS_CARGO_CLOSURE_PROOF_FAILED:${error.message}`);
    process.exitCode = 1;
  }
}

module.exports = {verify, candidateFiles, normalizedCargo, verifyEngineInputs, verifyUnchangedOwnerInputs};

'use strict';
const path = require('node:path');
const {verify, verifyEngineInputs, verifyUnchangedOwnerInputs} = require('./ci-cargo-closure-proof.cjs');
// These are the CI-only changes introduced *after* the last fully tested
// Windows binary baseline. They cannot affect Rust production crate output.
// They still trigger full CI if changed in the current PR (file classifier).
const baselineCiOnly = new Set([
  '.github/workflows/ci.yml',
  'scripts/check_ci_native_scope.py',
  'scripts/prepare-research-release.ps1',
  'scripts/research-release-scope.cjs',
  'scripts/test-research-release-scope.cjs',
]);
const safe = file => typeof file === 'string' && file.length <= 512 && !file.startsWith('/') && !file.includes('\\') && file.split('/').every(p => /^[A-Za-z0-9_.-]+$/.test(p) && p !== '.' && p !== '..');
const nonNative = file => file === 'README.md' || (file.startsWith('docs/') && file.endsWith('.md')) || file.startsWith('cloudflare/okx-mcp/') || baselineCiOnly.has(file);
const researchSource = file => /^crates\/okx-(analysis|research-service|research|strategy-core|research-domain|research-protocol)\/(src|tests)\/.+\.rs$/.test(file);
const protectedResearchSource = file => /^crates\/okx-research-(domain|protocol)\/(src|tests)\/.+\.rs$/.test(file);
const hostSource = file => /^crates\/okx-host-(controller|launcher|protocol)\/(src|tests)\/.+\.rs$/.test(file);
const engineSource = file => /^crates\/okx-(trading-engine|trade-execution)\/(src|tests)\/.+\.rs$/.test(file);
const researchManifest = file => ['research', 'research-service', 'strategy-core',
  'analysis', 'research-domain', 'research-protocol'].some(name => file === `crates/okx-${name}/Cargo.toml`);
const hostManifest = file => ['controller', 'launcher'].some(name => file === `crates/okx-host-${name}/Cargo.toml`);
const needsFinancialMatrix = file => [
  'crates/okx-research-service/src/main.rs',
  'crates/okx-research-service/src/process_memory.rs',
  'crates/okx-host-controller/src/job.rs',
  'crates/okx-host-controller/src/lifecycle_executor.rs',
].includes(file);

function checkEngine(baseMetadata, metadata, changed) {
  if (!Array.isArray(changed) || changed.length < 1 || changed.length > 500 ||
      new Set(changed).size !== changed.length || !changed.every(safe) ||
      !changed.some(engineSource) ||
      !changed.every(file => engineSource(file) || nonNative(file)))
    throw Error('ENGINE_ONLY_ACCEPTED_BASE_DIFF_INVALID');
  // Reuse is bound to the actual accepted Native base, not just the PR base.
  // Compare normal/build/dev closures and changed paths of *all* other
  // deployable Windows owners. A dev-only consumer is still a real compiler
  // dependency and must prevent a false source-isolation claim.
  const protectedDirs = verifyUnchangedOwnerInputs(baseMetadata, metadata, changed,
    ['okx-research-service', 'okx-host-controller', 'okx-host-launcher']);
  return {scope:'engine', protected_workspace_dirs: protectedDirs,
    changed_files:changed.length, other_windows_cargo_closures_identical:true,
    release_is_complete:false};
}

function checkMixed(baseMetadata, metadata, changed) {
  if (!Array.isArray(changed) || changed.length < 2 || changed.length > 500 ||
      new Set(changed).size !== changed.length || !changed.every(safe) ||
      !changed.some(file => researchSource(file) || protectedResearchSource(file) || researchManifest(file)) ||
      !changed.some(file => hostSource(file) || hostManifest(file)))
    throw Error('INVALID_MIXED_OWNER_DIFF');
  if (changed.some(needsFinancialMatrix) ||
      !changed.every(file => researchSource(file) || protectedResearchSource(file) || researchManifest(file) ||
        hostSource(file) || hostManifest(file) || file === 'Cargo.lock' || nonNative(file)))
    throw Error('OUTSIDE_MIXED_OWNER_SCOPE');
  const protectedDirs = verifyEngineInputs(baseMetadata, metadata, changed);
  return {
    scope: 'research-host',
    protected_workspace_dirs: protectedDirs,
    changed_files: changed.length,
    engine_windows_cargo_closure_identical: true,
  };
}

function check(metadata, changed, cargoProof = null) {
  if (!metadata.resolve || !Array.isArray(metadata.packages) || !Array.isArray(metadata.resolve.nodes)) throw Error('METADATA_RESOLUTION_REQUIRED');
  const packages = new Map(metadata.packages.map(p => [p.id, p]));
  const nodes = new Map(metadata.resolve.nodes.map(n => [n.id, n]));
  const roots = ['okx-trading-engine', 'okx-host-controller', 'okx-host-launcher'].map(name => {
    const found = metadata.packages.filter(p => p.name === name && metadata.workspace_members.includes(p.id));
    if (found.length !== 1) throw Error('PRODUCTION_ROOT_MISSING');
    return found[0].id;
  });
  const seen = new Set(), queue = [...roots];
  while (queue.length) {
    const id = queue.pop(); if (seen.has(id)) continue; seen.add(id);
    if (!packages.has(id)) throw Error('DEPENDENCY_PACKAGE_MISSING');
    const node = nodes.get(id); if (!node || !Array.isArray(node.deps)) throw Error('DEPENDENCY_GRAPH_INCOMPLETE');
    for (const dep of node.deps) if (dep.dep_kinds.some(k => k.kind === null || k.kind === 'normal' || k.kind === 'build')) queue.push(dep.pkg);
  }
  const workspace = path.resolve(metadata.workspace_root);
  const protectedDirs = [...seen].map(id => packages.get(id)).filter(p => p.source == null).map(p => {
    const relative = path.relative(workspace, path.dirname(p.manifest_path)).split(path.sep).join('/');
    if (!relative || relative.startsWith('../') || path.isAbsolute(relative)) throw Error('UNSAFE_WORKSPACE_PACKAGE');
    return relative + '/';
  });
  if (!Array.isArray(changed) || !changed.length || changed.length > 500) throw Error('EMPTY_OR_UNBOUNDED_RESEARCH_DIFF');
  const manifests = new Set(['crates/okx-research/Cargo.toml', 'crates/okx-analysis/Cargo.toml',
    'crates/okx-research-service/Cargo.toml', 'crates/okx-strategy-core/Cargo.toml',
    'crates/okx-research-domain/Cargo.toml', 'crates/okx-research-protocol/Cargo.toml']);
  const dependencyChange = changed.some(file => file === 'Cargo.lock' || file.endsWith('/Cargo.toml'));
  const protectedSourceChange = changed.some(protectedResearchSource);
  if ((dependencyChange || protectedSourceChange) && (!cargoProof || cargoProof.schema !== 'okx.560.windows-cargo-closure-proof/v1' ||
      cargoProof.scope !== 'research' || cargoProof.target !== 'x86_64-pc-windows-msvc' ||
      cargoProof.protected_closures_identical !== true ||
      (!protectedSourceChange && !changed.some(file => manifests.has(file)))))
    throw Error('RESEARCH_CHANGE_WITHOUT_PROTECTED_WINDOWS_CLOSURE_PROOF');
  let touchedResearch = false;
  for (const file of changed) {
    if (!safe(file)) throw Error('INVALID_DIFF_PATH');
    // Release selector is outside the Research tree and never exempted in a
    // new PR. The baseline itself is now a permanent GitHub Release.
    if (file === 'Cargo.lock' && cargoProof) continue;
    if (manifests.has(file) && cargoProof) { touchedResearch = true; continue; }
    if (file === 'Cargo.lock' || file.endsWith('/Cargo.toml') || file === 'Cargo.toml' || file.startsWith('.cargo/')) throw Error('DEPENDENCY_CONFIGURATION_CHANGED');
    if (protectedDirs.some(dir => file.startsWith(dir))) throw Error('PRODUCTION_DEPENDENCY_CHANGED');
    if (file.startsWith('crates/okx-analysis/') || file.startsWith('crates/okx-research-service/') || file.startsWith('crates/okx-research/') || file.startsWith('crates/okx-strategy-core/') || file.startsWith('crates/okx-research-protocol/') || file.startsWith('crates/okx-research-domain/')) {
      if (file.endsWith('/build.rs')) throw Error('RESEARCH_BUILD_SCRIPT_CHANGED');
      touchedResearch = true;
      continue;
    }
    if (!nonNative(file)) throw Error('OUTSIDE_RESEARCH_SCOPE');
  }
  if (!touchedResearch) throw Error('EMPTY_RESEARCH_DIFF');
  return { production_packages: seen.size, protected_workspace_dirs: protectedDirs.sort(), changed_files: changed.length };
}
if (require.main === module) {
  const fs = require('node:fs');
  try {
    if (![4, 5, 6].includes(process.argv.length)) throw Error('RESEARCH_SCOPE_ARGUMENTS_INVALID');
    const metadata = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
    const changed = JSON.parse(fs.readFileSync(process.argv[3], 'utf8'));
    const baseline = process.argv[4] ? JSON.parse(fs.readFileSync(process.argv[4], 'utf8')) : null;
    if (process.argv[5] !== undefined && !['--mixed-host', '--engine-only'].includes(process.argv[5]))
      throw Error('RESEARCH_SCOPE_ARGUMENTS_INVALID');
    const result = process.argv[5] === '--engine-only' ? checkEngine(baseline, metadata, changed) :
      process.argv[5] === '--mixed-host' ? checkMixed(baseline, metadata, changed) :
      check(metadata, changed, baseline ? verify(baseline, metadata, changed, 'research') : null);
    process.stdout.write(JSON.stringify(result) + '\n');
  }
  catch (e) { console.error(e.message); process.exitCode = 1; }
}
module.exports = { check, checkMixed, checkEngine };

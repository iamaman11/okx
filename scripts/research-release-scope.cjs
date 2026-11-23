'use strict';
const path = require('node:path');
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
function check(metadata, changed) {
  if (!metadata.resolve || !Array.isArray(metadata.packages) || !Array.isArray(metadata.resolve.nodes)) throw Error('METADATA_RESOLUTION_REQUIRED');
  const packages = new Map(metadata.packages.map(p => [p.id, p]));
  const nodes = new Map(metadata.resolve.nodes.map(n => [n.id, n]));
  const roots = ['okx-agent', 'okx-host-control', 'okx-host-launcher'].map(name => {
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
  let touchedResearch = false;
  for (const file of changed) {
    if (!safe(file)) throw Error('INVALID_DIFF_PATH');
    // Release selector is outside the Research tree and never exempted in a
    // new PR. The baseline itself is now a permanent GitHub Release.
    if (file === 'Cargo.lock' || file.endsWith('/Cargo.toml') || file === 'Cargo.toml' || file.startsWith('.cargo/')) throw Error('DEPENDENCY_CONFIGURATION_CHANGED');
    if (protectedDirs.some(dir => file.startsWith(dir))) throw Error('PRODUCTION_DEPENDENCY_CHANGED');
    if (file.startsWith('crates/okx-research-worker/') || file.startsWith('crates/okx-research/') || file.startsWith('crates/okx-strategy-core/')) {
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
  try { const metadata = JSON.parse(fs.readFileSync(process.argv[2], 'utf8')); const changed = JSON.parse(fs.readFileSync(process.argv[3], 'utf8')); process.stdout.write(JSON.stringify(check(metadata, changed)) + '\n'); }
  catch (e) { console.error(e.message); process.exitCode = 1; }
}
module.exports = { check };

'use strict';

// Diagnostic-only impact evidence for #560. The protected CI selector remains
// fail-closed and MUST NOT consume this report as permission to skip Native.
// Cargo metadata is already produced by the existing dependency-audit job.
const fs = require('node:fs');
const path = require('node:path');

const RELEASE_ROOTS = Object.freeze([
  ['okx-trading-engine', 'okx-agent.exe'],
  ['okx-research-service', 'okx-research-service.exe'],
  ['okx-host-controller', 'okx-host-control.exe'],
  ['okx-host-launcher', 'okx-host-launcher.exe'],
]);

const validPath = value => typeof value === 'string' && value.length >= 1 &&
  value.length <= 512 && !value.includes('\\') &&
  value.split('/').every(p => p !== '.' && p !== '..' && /^[A-Za-z0-9_.-]+$/.test(p));

function changedFiles(pages) {
  if (pages === null) return {files: null, reason: 'PUSH_DIFF_NOT_SUPPLIED'};
  if (!Array.isArray(pages) || pages.length < 1 || pages.length > 30)
    return {files: null, reason: 'DIFF_PAGINATION_UNVERIFIED'};
  const files = [], seen = new Set();
  for (const page of pages) {
    if (!Array.isArray(page) || page.length > 100)
      return {files: null, reason: 'DIFF_PAGE_UNVERIFIED'};
    for (const item of page) {
      if (!item || !validPath(item.filename) || !['added', 'modified', 'removed'].includes(item.status)
          || Object.hasOwn(item, 'previous_filename') || seen.has(item.filename))
        return {files: null, reason: 'DIFF_ENTRY_UNVERIFIED'};
      files.push(item.filename);
      seen.add(item.filename);
    }
  }
  return files.length > 0 && files.length <= 500
    ? {files, reason: null} : {files: null, reason: 'DIFF_SIZE_UNVERIFIED'};
}

function graph(metadata) {
  if (!metadata || !path.isAbsolute(metadata.workspace_root) ||
      !Array.isArray(metadata.packages) || !Array.isArray(metadata.workspace_members) ||
      !metadata.resolve || !Array.isArray(metadata.resolve.nodes))
    throw Error('CARGO_METADATA_RESOLUTION_REQUIRED');
  const packages = new Map();
  for (const pkg of metadata.packages) {
    if (typeof pkg.id !== 'string' || packages.has(pkg.id)) throw Error('DUPLICATE_PACKAGE_ID');
    packages.set(pkg.id, pkg);
  }
  const nodes = new Map();
  for (const node of metadata.resolve.nodes) {
    if (typeof node.id !== 'string' || nodes.has(node.id) || !Array.isArray(node.deps))
      throw Error('INVALID_CARGO_NODE');
    nodes.set(node.id, node);
  }
  const workspace = new Map();
  for (const id of metadata.workspace_members) {
    const pkg = packages.get(id);
    if (!pkg || typeof pkg.name !== 'string' || typeof pkg.manifest_path !== 'string')
      throw Error('INVALID_WORKSPACE_MEMBER');
    const relative = path.relative(metadata.workspace_root, path.dirname(pkg.manifest_path))
      .split(path.sep).join('/');
    if (!relative.startsWith('crates/') || !validPath(relative))
      throw Error('UNSAFE_WORKSPACE_PACKAGE_PATH');
    if (workspace.has(relative)) throw Error('DUPLICATE_WORKSPACE_PATH');
    workspace.set(relative, pkg);
    if (!nodes.has(id)) throw Error('WORKSPACE_MEMBER_GRAPH_MISSING');
  }
  const roots = RELEASE_ROOTS.map(([name, binary]) => {
    const found = [...workspace.values()].filter(pkg => pkg.name === name);
    if (found.length !== 1) throw Error(`RELEASE_ROOT_MISSING:${name}`);
    return {name, binary, id: found[0].id};
  });
  function dependencyPath(root, wanted) {
    const queue = [[root]], seen = new Set();
    while (queue.length) {
      const chain = queue.shift(), id = chain.at(-1);
      if (id === wanted) return chain.map(key => packages.get(key)?.name ?? key);
      if (seen.has(id)) continue;
      seen.add(id);
      const node = nodes.get(id);
      if (!node || !packages.has(id)) throw Error(`CARGO_GRAPH_INCOMPLETE:${id}`);
      for (const dep of node.deps) {
        if (!Array.isArray(dep.dep_kinds) || !packages.has(dep.pkg))
          throw Error('CARGO_DEPENDENCY_KIND_MISSING');
        // Conservative over all target configurations, including Windows.
        // Only normal/build edges affect release binaries; dev-only edges do not.
        if (dep.dep_kinds.some(k => k && (k.kind === null || k.kind === 'normal' || k.kind === 'build')))
          queue.push([...chain, dep.pkg]);
      }
    }
    return null;
  }
  return {workspace, roots, dependencyPath};
}

function analyze(metadata, pages) {
  const cargo = graph(metadata);
  const {files, reason} = changedFiles(pages);
  const changed = new Map(), other = new Set(), unknown = [];
  if (files) for (const file of files) {
    if (file === 'Cargo.lock' || file === 'Cargo.toml' || file.startsWith('.cargo/') ||
        file.endsWith('/Cargo.toml') || file.endsWith('/build.rs')) {
      unknown.push(file);
      continue;
    }
    if (file === 'README.md' || (file.startsWith('docs/') && file.endsWith('.md'))) continue;
    if (file.startsWith('cloudflare/okx-mcp/')) {
      other.add('cloudflare-mcp');
      continue;
    }
    const owner = [...cargo.workspace.entries()].find(([dir]) => file.startsWith(`${dir}/`));
    if (owner) changed.set(owner[1].id, owner[1].name);
    else unknown.push(file);
  }
  const unverified = reason || (unknown.length ? 'UNKNOWN_OR_DEPENDENCY_CONFIGURATION_CHANGE' : null);
  const binaries = cargo.roots.map(root => ({
    binary: root.binary,
    package: root.name,
    paths: unverified ? [] : [...changed.keys()].map(id => cargo.dependencyPath(root.id, id))
      .filter(Boolean),
  })).filter(root => unverified || root.paths.length);
  return {
    schema: 'okx.560.cargo-impact-evidence/v1',
    authority: 'DIAGNOSTIC_ONLY_NOT_A_CI_SKIP_PROOF',
    dependency_edges: 'normal_and_build_all_target_cfgs_without_dev',
    diff_verified: files !== null,
    graph_complete: true,
    uncertainty: unverified,
    changed_files: files ?? [],
    changed_workspace_packages: [...changed.values()].sort(),
    other_components: [...other].sort(),
    unknown_paths: unknown,
    potentially_affected_windows_binaries: binaries,
  };
}

function markdown(result) {
  const lines = [
    '### Cargo impact inventory (#560)',
    '',
    'Diagnostic only. The existing protected classifier decides CI execution; this report never exempts financial gates.',
    '',
    `Diff verified: ${result.diff_verified}; current Cargo graph: ${result.graph_complete}; uncertainty: ${result.uncertainty ?? 'none'}.`,
    '',
    `Changed workspace crates: ${result.changed_workspace_packages.join(', ') || 'none'}.`,
    `Other components: ${result.other_components.join(', ') || 'none'}.`,
    '',
    '| Potential Windows binary | Normal/build dependency paths from binary to change |',
    '| --- | --- |',
  ];
  for (const binary of result.potentially_affected_windows_binaries) {
    lines.push(`| \`${binary.binary}\` | ${result.uncertainty ? 'Unverified impact — no narrow-build claim' :
      binary.paths.map(p => p.map(x => `\`${x}\``).join(' → ')).join('; ')} |`);
  }
  if (!result.potentially_affected_windows_binaries.length)
    lines.push('| None from current Cargo normal/build graph | No Rust release binary dependency found |');
  lines.push('', 'A Cargo manifest/lock edit needs a separately verified before/after resolver and feature comparison before any scoped exemption.', '');
  return lines.join('\n');
}

if (require.main === module) {
  try {
    if (process.argv.length < 3 || process.argv.length > 5)
      throw Error('USAGE: node ci-component-impact.cjs <cargo-metadata.json> [pr-files-pages.json] [--markdown]');
    const metadata = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
    const fileArg = process.argv.slice(3).find(v => v !== '--markdown');
    const pages = fileArg ? JSON.parse(fs.readFileSync(fileArg, 'utf8')) : null;
    const result = analyze(metadata, pages);
    process.stdout.write(process.argv.includes('--markdown') ? markdown(result) : JSON.stringify(result) + '\n');
  } catch (e) {
    console.error(`CARGO_IMPACT_EVIDENCE_FAILED:${e.message}`);
    process.exitCode = 1;
  }
}

module.exports = {analyze, changedFiles, markdown};

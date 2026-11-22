'use strict';
// Candidate-only Research v2 composition. The installed CONTROL remains the
// authority for GitHub-origin, installed-byte and active-version checks.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const REPOSITORY_ID = 1388071566;
const BASE_NAMES = ['okx-agent.exe', 'okx-host-control.exe', 'okx-host-launcher.exe'];
const WORKER = 'okx-research-worker.exe'; // Current installed CONTROL identity.
const NAMES = [...BASE_NAMES, WORKER];
const FILES = ['Cargo.lock', 'manifest.json', ...NAMES];
const MANIFEST_FIELDS = ['schema', 'repository_id', 'source_head_sha', 'source_tree', 'rust_version', 'components'];
const COMPONENT_FIELDS = ['sha256', 'size', 'source_head_sha', 'source_tree', 'run_id', 'artifact_id', 'protocol_version', 'storage_version'];
const hex = (s, n) => typeof s === 'string' && new RegExp(`^[0-9a-f]{${n}}$`).test(s);
const sha256 = b => crypto.createHash('sha256').update(b).digest('hex');
const fail = code => { throw new Error(code); };
const sameKeys = (value, names) => value && typeof value === 'object' && !Array.isArray(value) &&
  JSON.stringify(Object.keys(value).sort()) === JSON.stringify([...names].sort());
const goodId = v => Number.isSafeInteger(v) && v > 0;

function validateOrigin(bundle, origin) {
  const m = bundle.manifest;
  if (!sameKeys(bundle.files, FILES) || !sameKeys(m, MANIFEST_FIELDS) || !sameKeys(m.components, NAMES) ||
      m.schema !== 'okx.windows.bundle/v2' || m.repository_id !== REPOSITORY_ID ||
      m.source_head_sha !== origin.head || m.source_tree !== origin.tree ||
      !hex(origin.head, 40) || !hex(origin.tree, 40) ||
      !goodId(origin.run_id) || !goodId(origin.artifact_id) ||
      typeof m.rust_version !== 'string' || !m.rust_version.trim()) fail('ORIGIN_MANIFEST_INVALID');
  for (const name of NAMES) {
    const c = m.components[name];
    const bytes = bundle.files[name];
    if (!sameKeys(c, COMPONENT_FIELDS) || !Buffer.isBuffer(bytes) ||
        !hex(c.sha256, 64) || c.size !== bytes.length || bytes.length === 0 ||
        bytes.length > 128 * 1024 * 1024 || sha256(bytes) !== c.sha256 ||
        c.source_head_sha !== origin.head || c.source_tree !== origin.tree ||
        c.run_id !== 0 || c.artifact_id !== 0 ||
        c.protocol_version !== 1 || c.storage_version !== 1) fail(`ORIGIN_COMPONENT_INVALID:${name}`);
  }
  if (!Buffer.isBuffer(bundle.files['Cargo.lock']) || bundle.files['Cargo.lock'].length > 4 * 1024 * 1024)
    fail('ORIGIN_LOCK_INVALID');
}

function validateBaseline(baseline, base, baseOrigin) {
  if (!sameKeys(baseline, ['schema','repository_id','run_id','artifact_id','source_head_sha','source_tree','components']) ||
      baseline.schema !== 'okx.537.installed-native-base/v1' ||
      baseline.repository_id !== REPOSITORY_ID ||
      baseline.run_id !== baseOrigin.run_id || baseline.artifact_id !== baseOrigin.artifact_id ||
      baseline.source_head_sha !== baseOrigin.head || baseline.source_tree !== baseOrigin.tree ||
      !sameKeys(baseline.components, BASE_NAMES)) fail('INSTALLED_BASE_ORIGIN_MISMATCH');
  for (const name of BASE_NAMES) {
    if (!hex(baseline.components[name], 64) || baseline.components[name] !== base.manifest.components[name].sha256)
      fail(`INSTALLED_BASE_HASH_MISMATCH:${name}`);
  }
}

function validateCandidate(candidate, base, research, baseOrigin, researchOrigin, baseline, candidateHead, candidateTree) {
  validateOrigin(base, baseOrigin);
  validateOrigin(research, researchOrigin);
  validateBaseline(baseline, base, baseOrigin);
  const m = candidate.manifest;
  if (!sameKeys(candidate.files, FILES) || !sameKeys(m, MANIFEST_FIELDS) || !sameKeys(m.components, NAMES) ||
      m.schema !== 'okx.windows.bundle/v2' || m.repository_id !== REPOSITORY_ID ||
      !hex(candidateHead, 40) || !hex(candidateTree, 40) ||
      m.source_head_sha !== candidateHead || m.source_tree !== candidateTree ||
      typeof m.rust_version !== 'string' || !m.rust_version.trim()) fail('COMPOSITION_IDENTITY_MISMATCH');
  for (const name of NAMES) {
    const origin = name === WORKER ? researchOrigin : baseOrigin;
    const from = name === WORKER ? research : base;
    const expected = {...from.manifest.components[name],
      run_id: origin.run_id, artifact_id: origin.artifact_id};
    const c = m.components[name];
    const bytes = candidate.files[name];
    if (!sameKeys(c, COMPONENT_FIELDS) || !sameKeys(expected, COMPONENT_FIELDS) ||
        COMPONENT_FIELDS.some(field => c[field] !== expected[field])) fail(`COMPONENT_ORIGIN_MISMATCH:${name}`);
    if (!Buffer.isBuffer(bytes) || bytes.length !== c.size || sha256(bytes) !== c.sha256 ||
        !bytes.equals(from.files[name])) fail(`COMPONENT_BYTES_MISMATCH:${name}`);
  }
  if (!candidate.files['Cargo.lock'].equals(research.files['Cargo.lock'])) fail('COMPOSITION_LOCK_MISMATCH');
  // In the real DeployResearch the main tree must equal candidateTree. A Draft
  // proves the prospective tree only; it is not yet eligible for activation.
  return true;
}

function compose(base, research, baseOrigin, researchOrigin, baseline, candidateHead, candidateTree) {
  validateOrigin(base, baseOrigin);
  validateOrigin(research, researchOrigin);
  validateBaseline(baseline, base, baseOrigin);
  const components = {};
  const files = {'Cargo.lock': research.files['Cargo.lock']};
  for (const name of NAMES) {
    const source = name === WORKER ? research : base;
    const origin = name === WORKER ? researchOrigin : baseOrigin;
    components[name] = {...source.manifest.components[name],
      run_id: origin.run_id, artifact_id: origin.artifact_id};
    files[name] = source.files[name];
  }
  const manifest = {schema: 'okx.windows.bundle/v2', repository_id: REPOSITORY_ID,
    source_head_sha: candidateHead, source_tree: candidateTree,
    rust_version: research.manifest.rust_version, components};
  files['manifest.json'] = Buffer.from(JSON.stringify(manifest, null, 2) + '\n');
  const candidate = {manifest, files};
  validateCandidate(candidate, base, research, baseOrigin, researchOrigin, baseline, candidateHead, candidateTree);
  return candidate;
}

function readBundle(directory) {
  if (!path.isAbsolute(directory) ||
      JSON.stringify(fs.readdirSync(directory).sort()) !== JSON.stringify([...FILES].sort())) fail('UNSUPPORTED_BUNDLE_ENTRIES');
  const files = {};
  for (const name of FILES) {
    const file = path.join(directory, name);
    if (!fs.lstatSync(file).isFile()) fail(`UNSUPPORTED_BUNDLE_ENTRY:${name}`);
    files[name] = fs.readFileSync(file);
  }
  return {files, manifest: JSON.parse(files['manifest.json'].toString('utf8'))};
}

function main(argv) {
  if (argv.length !== 3) fail('USAGE: node compose-accepted-research-v2.cjs <input.json> <dist> <proof.json>');
  const [inputPath, dist, proofPath] = argv;
  const input = JSON.parse(fs.readFileSync(inputPath, 'utf8'));
  const base = readBundle(input.base_dir);
  const research = readBundle(input.research_dir);
  const baseline = JSON.parse(fs.readFileSync(input.baseline_file, 'utf8'));
  const candidate = compose(base, research, input.base_origin, input.research_origin,
    baseline, input.candidate_head, input.candidate_tree);
  if (!path.isAbsolute(dist) || !path.isAbsolute(proofPath) ||
      (fs.existsSync(dist) && fs.readdirSync(dist).length)) fail('DESTINATION_NOT_EMPTY');
  fs.mkdirSync(dist, {recursive: true});
  for (const name of FILES) fs.writeFileSync(path.join(dist, name), candidate.files[name], {flag:'wx'});
  const proof = {schema:'okx.537.research-composition-proof/v1',
    candidate_head: input.candidate_head, candidate_tree: input.candidate_tree,
    base_origin: input.base_origin, research_origin: input.research_origin,
    installed_base_match: true, control_candidate_rules: 'PASS_OFFLINE_NOT_ACTIVATED',
    files: Object.fromEntries(FILES.map(name => [name, {sha256:sha256(candidate.files[name]),size:candidate.files[name].length}]))};
  fs.writeFileSync(proofPath, JSON.stringify(proof, null, 2) + '\n', {flag:'wx'});
  process.stdout.write(`T10_COMPOSITION_V2_OFFLINE=PASS worker_sha256=${candidate.manifest.components[WORKER].sha256}\n`);
}

if (require.main === module) {
  try { main(process.argv.slice(2)); } catch (e) { console.error(e.message); process.exitCode = 1; }
}
module.exports = {BASE_NAMES, WORKER, NAMES, FILES, compose, validateCandidate, validateOrigin, validateBaseline, sha256};

'use strict';

// Exact three-file Research component format. An Actions artifact is itself a
// ZIP; the producer uploads these files directly, without a nested archive.
// This is a local byte/schema proof, not a substitute for the consumer's
// authenticated GitHub successful-PR and exact merged-main-tree checks.
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

const FILES = Object.freeze(['Cargo.lock', 'manifest.json', 'okx-research-service.exe']);
const SHA40 = /^[0-9a-f]{40}$/;
const SHA64 = /^[0-9a-f]{64}$/;
const sha256 = bytes => crypto.createHash('sha256').update(bytes).digest('hex');

function exactKeys(value, keys, cause) {
  if (!value || typeof value !== 'object' || Array.isArray(value) ||
      Object.getPrototypeOf(value) !== Object.prototype ||
      JSON.stringify(Object.keys(value).sort()) !== JSON.stringify([...keys].sort()))
    throw Error(cause);
}

function validateManifest(manifest, lockBytes, workerBytes, expectedHead, expectedTree) {
  exactKeys(manifest, ['schema', 'repository_id', 'source_head_sha', 'source_tree',
    'rust_version', 'cargo_lock_sha256', 'worker'], 'RESEARCH_COMPONENT_MANIFEST_FIELDS');
  exactKeys(manifest.worker, ['sha256', 'size', 'protocol_version', 'storage_version'],
    'RESEARCH_COMPONENT_WORKER_FIELDS');
  if (manifest.schema !== 'okx.windows.research-component/v1' ||
      manifest.repository_id !== 1388071566 ||
      !SHA40.test(manifest.source_head_sha) || !SHA40.test(manifest.source_tree) ||
      !SHA40.test(expectedHead) || !SHA40.test(expectedTree) ||
      manifest.source_head_sha !== expectedHead || manifest.source_tree !== expectedTree ||
      typeof manifest.rust_version !== 'string' ||
      !/^rustc 1\.100\.0(?:\s|$)/.test(manifest.rust_version) ||
      manifest.rust_version.length > 160 ||
      !SHA64.test(manifest.cargo_lock_sha256) ||
      manifest.cargo_lock_sha256 !== sha256(lockBytes))
    throw Error('RESEARCH_COMPONENT_SOURCE_OR_LOCK_INVALID');
  const c = manifest.worker;
  if (!SHA64.test(c.sha256) || !Number.isSafeInteger(c.size) ||
      c.size <= 0 || c.size > 134217728 || c.size !== workerBytes.length ||
      c.sha256 !== sha256(workerBytes) || c.protocol_version !== 1 ||
      c.storage_version !== 1)
    throw Error('RESEARCH_COMPONENT_BYTES_OR_COMPATIBILITY_INVALID');
  return {schema: manifest.schema, source_head_sha: expectedHead,
    source_tree: expectedTree, research_component_sha256: c.sha256,
    cargo_lock_sha256: manifest.cargo_lock_sha256,
    release_is_complete: false, no_engine_binary_or_dependency_base: true};
}

function verifyDirectory(directory, expectedHead, expectedTree) {
  if (!path.isAbsolute(directory) || !fs.statSync(directory).isDirectory())
    throw Error('RESEARCH_COMPONENT_DIRECTORY_INVALID');
  const entries = fs.readdirSync(directory).sort();
  if (JSON.stringify(entries) !== JSON.stringify(FILES))
    throw Error('RESEARCH_COMPONENT_EXACT_THREE_FILES_REQUIRED');
  const sizes = {'Cargo.lock': 4 * 1024 * 1024, 'manifest.json': 64 * 1024,
    'okx-research-service.exe': 134217728};
  const bytes = {};
  for (const name of FILES) {
    const file = path.join(directory, name), stat = fs.lstatSync(file);
    if (!stat.isFile() || stat.size < 1 || stat.size > sizes[name])
      throw Error(`RESEARCH_COMPONENT_FILE_NOT_REGULAR_OR_BOUNDED:${name}`);
    bytes[name] = fs.readFileSync(file);
  }
  return validateManifest(JSON.parse(bytes['manifest.json'].toString('utf8')),
    bytes['Cargo.lock'], bytes['okx-research-service.exe'], expectedHead, expectedTree);
}

if (require.main === module) {
  try {
    if (process.argv.length !== 5)
      throw Error('USAGE: research-component-artifact <directory> <head40> <tree40>');
    process.stdout.write(JSON.stringify(verifyDirectory(...process.argv.slice(2))) + '\n');
  } catch (error) {
    console.error(`RESEARCH_COMPONENT_ARTIFACT_REJECTED:${error.message}`);
    process.exitCode = 1;
  }
}

module.exports = {FILES, sha256, validateManifest, verifyDirectory};

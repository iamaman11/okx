'use strict';
const crypto = require('node:crypto');
function createProvenance(input, manifest, archiveBytes) {
  const hex40 = /^[a-f0-9]{40}$/, hex64 = /^[a-f0-9]{64}$/;
  const sha = crypto.createHash('sha256').update(archiveBytes).digest('hex');
  if (input.repository_id !== 1388071566 || !hex40.test(input.source_head_sha) || !hex40.test(input.source_tree) || !hex40.test(input.merged_main_sha) ||
      !Number.isSafeInteger(input.run_id) || input.run_id < 1 || !Number.isSafeInteger(input.artifact_id) || input.artifact_id < 1 ||
      !hex64.test(input.artifact_sha256) || sha !== input.artifact_sha256) throw Error('INVALID_ARTIFACT_EVIDENCE');
  if (!manifest || manifest.repository_id !== input.repository_id || manifest.source_head_sha !== input.source_head_sha || manifest.source_tree !== input.source_tree) throw Error('MANIFEST_ORIGIN_MISMATCH');
  const components = {};
  const files = ['okx-agent.exe', 'okx-host-control.exe', 'okx-host-launcher.exe'];
  if (manifest.schema === 'okx.windows.bundle/v2') files.push('okx-research-worker.exe');
  else if (manifest.schema !== 'okx.windows.bundle/v1') throw Error('UNSUPPORTED_MANIFEST');
  for (const name of files) {
    const item = manifest.schema.endsWith('/v2') ? manifest.components?.[name] : manifest.files?.[name];
    if (!item || !hex64.test(item.sha256)) throw Error('INVALID_COMPONENT');
    if (manifest.schema.endsWith('/v2')) {
      if (!Number.isSafeInteger(item.size) || item.size < 1 || item.source_head_sha !== input.source_head_sha || item.source_tree !== input.source_tree ||
          item.run_id !== 0 || item.artifact_id !== 0 || item.protocol_version !== 1 || item.storage_version !== 1) throw Error('INVALID_FRESH_ORIGIN');
      components[name] = {sha256:item.sha256,size:item.size,source_head_sha:input.source_head_sha,source_tree:input.source_tree,run_id:input.run_id,artifact_id:input.artifact_id,protocol_version:1,storage_version:1};
    } else components[name] = {sha256:item.sha256,source_head_sha:input.source_head_sha,source_tree:input.source_tree,run_id:input.run_id,artifact_id:input.artifact_id};
  }
  if (Object.keys(manifest.schema.endsWith('/v2') ? manifest.components : manifest.files).length !== files.length) throw Error('INVALID_COMPONENT_SET');
  return {schema:'okx.published-origin/v1',repository_id:input.repository_id,source_head_sha:input.source_head_sha,source_tree:input.source_tree,merged_main_sha:input.merged_main_sha,run_id:input.run_id,artifact_id:input.artifact_id,archive_sha256:sha,components};
}
if (require.main === module) {
  const fs = require('node:fs');
  try {
    const [inputFile, manifestFile, archiveFile, outputFile] = process.argv.slice(2);
    const record = createProvenance(JSON.parse(fs.readFileSync(inputFile,'utf8')), JSON.parse(fs.readFileSync(manifestFile,'utf8')), fs.readFileSync(archiveFile));
    fs.writeFileSync(outputFile, JSON.stringify(record,null,2)+'\n', {flag:'wx'});
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
module.exports = { createProvenance };

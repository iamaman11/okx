'use strict';
const assert = require('node:assert/strict');
const {BASE_NAMES, WORKER, NAMES, FILES, compose, validateCandidate, verifyRealNegativeControls, sha256} =
  require('./compose-verified-research-v2.cjs');

const head = letter => letter.repeat(40);
const baseOrigin = {head:head('a'),tree:head('b'),run_id:101,artifact_id:201};
const researchOrigin = {head:head('c'),tree:head('d'),run_id:102,artifact_id:202};
const candidateHead = head('e'), candidateTree = head('f');
function fixture(origin) {
  const files = {'Cargo.lock':Buffer.from('lock'), 'manifest.json':Buffer.from('{}')};
  const components = {};
  for (const name of NAMES) {
    const bytes = Buffer.from(`origin:${origin.head}:${name}`);
    files[name] = bytes;
    components[name] = {sha256:sha256(bytes),size:bytes.length,
      source_head_sha:origin.head,source_tree:origin.tree,
      run_id:0,artifact_id:0,protocol_version:1,storage_version:1};
  }
  return {files,manifest:{schema:'okx.windows.bundle/v2',repository_id:1388071566,
    source_head_sha:origin.head,source_tree:origin.tree,rust_version:'rustc 1.100.0',components}};
}
const base=fixture(baseOrigin), research=fixture(researchOrigin);
const baseline={schema:'okx.537.installed-native-base/v1',repository_id:1388071566,
  run_id:baseOrigin.run_id,artifact_id:baseOrigin.artifact_id,
  source_head_sha:baseOrigin.head,source_tree:baseOrigin.tree,
  components:Object.fromEntries(BASE_NAMES.map(name=>[name,base.manifest.components[name].sha256]))};
const candidate=compose(base,research,baseOrigin,researchOrigin,baseline,candidateHead,candidateTree);
const verify = (c=candidate, b=base, r=research, bo=baseOrigin, ro=researchOrigin, installed=baseline, ch=candidateHead, ct=candidateTree) =>
  validateCandidate(c,b,r,bo,ro,installed,ch,ct);
const copied = () => ({manifest:structuredClone(candidate.manifest),files:{...candidate.files}});
assert.equal(verify(),true);
assert.equal(verifyRealNegativeControls(candidate,base,research,baseOrigin,researchOrigin,
  baseline,candidateHead,candidateTree).length,5);
assert.equal(candidate.manifest.components[WORKER].sha256,research.manifest.components[WORKER].sha256);
assert.equal(candidate.manifest.components[BASE_NAMES[0]].sha256,base.manifest.components[BASE_NAMES[0]].sha256);
assert.deepEqual(Object.keys(candidate.files).sort(),[...FILES].sort());

const wrongBase={...baseline,run_id:999};
assert.throws(()=>verify(candidate,base,research,baseOrigin,researchOrigin,wrongBase),/INSTALLED_BASE_ORIGIN_MISMATCH/);
const wrongInstalled={...baseline,components:{...baseline.components,[BASE_NAMES[0]]:'0'.repeat(64)}};
assert.throws(()=>verify(candidate,base,research,baseOrigin,researchOrigin,wrongInstalled),/INSTALLED_BASE_HASH_MISMATCH/);
let x=copied(); x.manifest.components[BASE_NAMES[0]].artifact_id=999;
assert.throws(()=>verify(x),/COMPONENT_ORIGIN_MISMATCH/); // Right bytes, false origin.
x=copied(); x.manifest.components[WORKER].source_head_sha='1'.repeat(40);
assert.throws(()=>verify(x),/COMPONENT_ORIGIN_MISMATCH/);
x=copied(); x.files[WORKER]=Buffer.from('damaged executable');
assert.throws(()=>verify(x),/COMPONENT_BYTES_MISMATCH/);
x=copied(); delete x.files[WORKER]; x.files['okx-research-service.exe']=candidate.files[WORKER];
assert.throws(()=>verify(x),/COMPOSITION_IDENTITY_MISMATCH/); // Not supported by installed CONTROL.
x=copied(); x.manifest.components['okx-research-service.exe']=x.manifest.components[WORKER]; delete x.manifest.components[WORKER];
assert.throws(()=>verify(x),/COMPOSITION_IDENTITY_MISMATCH/);
x=copied(); x.manifest.source_tree='1'.repeat(40);
assert.throws(()=>verify(x),/COMPOSITION_IDENTITY_MISMATCH/);
x=copied(); x.files['Cargo.lock']=Buffer.from('wrong lock');
assert.throws(()=>verify(x),/COMPOSITION_LOCK_MISMATCH/);
const damagedBase=fixture(baseOrigin); damagedBase.files[BASE_NAMES[0]]=Buffer.from('damaged');
assert.throws(()=>verify(candidate,damagedBase),/ORIGIN_COMPONENT_INVALID/);
const incompatibleResearch=fixture(researchOrigin); incompatibleResearch.manifest.components[WORKER].protocol_version=2;
assert.throws(()=>verify(candidate,base,incompatibleResearch),/ORIGIN_COMPONENT_INVALID/);
assert.throws(()=>verify(candidate,base,research,baseOrigin,{...researchOrigin,artifact_id:999}),/COMPONENT_ORIGIN_MISMATCH/);
assert.throws(()=>verify(candidate,base,research,baseOrigin,researchOrigin,baseline,candidateHead,'1'.repeat(40)),/COMPOSITION_IDENTITY_MISMATCH/);
console.log('T10_COMPOSITION_POSITIVE_AND_NEGATIVE=PASS 13 controls; installed version not activated');

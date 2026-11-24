'use strict';
const assert = require('node:assert/strict');
const {execFileSync} = require('node:child_process');
const {check} = require('./research-release-scope.cjs');
const root = process.cwd();
const packages = ['okx-trading-engine','okx-host-controller','okx-host-launcher','okx-ipc-contracts','okx-research-service','okx-analysis','okx-strategy-core'].map(name => ({id:name,name,manifest_path:require('node:path').join(root,'crates',name,'Cargo.toml')}));
const meta = {workspace_root:root,workspace_members:packages.map(p=>p.id),packages,resolve:{nodes:packages.map(p=>({id:p.id,deps:p.id==='okx-trading-engine'?[{pkg:'okx-ipc-contracts',dep_kinds:[{kind:null}]}]:[]}))}};
assert.equal(check(meta,['crates/okx-research-service/src/ipc.rs']).changed_files,1);
const verifiedManifest = {schema:'okx.560.windows-cargo-closure-proof/v1',
  scope:'research',target:'x86_64-pc-windows-msvc',protected_closures_identical:true};
assert.equal(check(meta,['crates/okx-research-service/Cargo.toml','Cargo.lock'],verifiedManifest).changed_files,2);
assert.throws(()=>check(meta,['Cargo.lock'],verifiedManifest),/RESEARCH_MANIFEST_WITHOUT/);
assert.throws(()=>check(meta,['crates/okx-research-service/Cargo.toml'],
  {...verifiedManifest,protected_closures_identical:false}),/RESEARCH_MANIFEST_WITHOUT/);
const pureCoreFile = 'crates/okx-strategy-core/src/operators.rs';
const analysisFile = 'crates/okx-analysis/src/lib.rs';
// Both Ready classifiers must agree on the same changed-file evidence.
for (const filename of [pureCoreFile, 'crates/okx-strategy-core/src/time_series.rs', analysisFile]) {
  assert.equal(check(meta,[filename]).changed_files,1);
  const pythonScope = execFileSync('python3',['-B','scripts/check_ci_native_scope.py'],{
    cwd:root,encoding:'utf8',input:JSON.stringify([[{filename,status:'modified'}]]),
  }).trim();
  assert.equal(pythonScope,'research');
}
assert.equal(check(meta,['.github/workflows/ci.yml', 'scripts/check_ci_native_scope.py', 'scripts/prepare-research-release.ps1', 'scripts/research-release-scope.cjs', 'scripts/test-research-release-scope.cjs', 'docs/acceptance/518/notes.md', 'cloudflare/okx-mcp/src/mcp.ts', 'crates/okx-research-service/src/analytics/market.rs']).changed_files,8);
assert.throws(()=>check(meta,['.github/research-release-base.json']));
assert.throws(()=>check(meta,['.github/workflows/ci.yml', 'docs/notes.md']));
assert.throws(()=>check({...meta, resolve:{nodes:[]}},['crates/okx-research-service/src/ipc.rs']));
for (const file of ['crates/okx-trading-engine/src/query.rs','crates/okx-ipc-contracts/src/lib.rs','Cargo.lock','crates/okx-research-service/Cargo.toml','.cargo/config.toml','crates/okx-market-runtime/src/lib.rs','scripts/published-release-origin.cjs','crates/okx-research-service/build.rs','docs/../secret.md']) assert.throws(()=>check(meta,['crates/okx-research-service/src/ipc.rs',file]),file);
meta.resolve.nodes.find(n=>n.id==='okx-trading-engine').deps.push({pkg:'okx-research-service',dep_kinds:[{kind:'build'}]});
assert.throws(()=>check(meta,['crates/okx-research-service/src/ipc.rs']));
const transitive = structuredClone(meta);
transitive.resolve.nodes.find(n=>n.id==='okx-trading-engine').deps = [{pkg:'okx-analysis',dep_kinds:[{kind:null}]}];
transitive.resolve.nodes.find(n=>n.id==='okx-analysis').deps.push({pkg:'okx-strategy-core',dep_kinds:[{kind:'normal'}]});
assert.throws(()=>check(transitive,[pureCoreFile]),/PRODUCTION_DEPENDENCY_CHANGED/);
assert.throws(()=>check(transitive,[analysisFile]),/PRODUCTION_DEPENDENCY_CHANGED/);
console.log('research release scope negative controls passed');

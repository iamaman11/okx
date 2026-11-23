'use strict';
const assert = require('node:assert/strict');
const {execFileSync} = require('node:child_process');
const {check} = require('./research-release-scope.cjs');
const root = process.cwd();
const packages = ['okx-agent','okx-host-control','okx-host-launcher','okx-protocol','okx-research-worker','okx-analysis','okx-strategy-core'].map(name => ({id:name,name,manifest_path:require('node:path').join(root,'crates',name,'Cargo.toml')}));
const meta = {workspace_root:root,workspace_members:packages.map(p=>p.id),packages,resolve:{nodes:packages.map(p=>({id:p.id,deps:p.id==='okx-agent'?[{pkg:'okx-protocol',dep_kinds:[{kind:null}]}]:[]}))}};
assert.equal(check(meta,['crates/okx-research-worker/src/ipc.rs']).changed_files,1);
const pureCoreFile = 'crates/okx-strategy-core/src/operators.rs';
assert.equal(check(meta,[pureCoreFile]).changed_files,1);
// Both Ready classifiers must agree on the same changed-file evidence.
const pythonScope = execFileSync('python3',['-B','scripts/check_ci_native_scope.py'],{
  cwd:root,encoding:'utf8',input:JSON.stringify([[{filename:pureCoreFile,status:'modified'}]]),
}).trim();
assert.equal(pythonScope,'research');
assert.equal(check(meta,['.github/workflows/ci.yml', 'scripts/check_ci_native_scope.py', 'scripts/prepare-research-release.ps1', 'scripts/research-release-scope.cjs', 'scripts/test-research-release-scope.cjs', 'docs/acceptance/518/notes.md', 'cloudflare/okx-mcp/src/mcp.ts', 'crates/okx-research-worker/src/analytics/market.rs']).changed_files,8);
assert.throws(()=>check(meta,['.github/research-release-base.json']));
assert.throws(()=>check(meta,['.github/workflows/ci.yml', 'docs/notes.md']));
assert.throws(()=>check({...meta, resolve:{nodes:[]}},['crates/okx-research-worker/src/ipc.rs']));
for (const file of ['crates/okx-agent/src/query.rs','crates/okx-protocol/src/lib.rs','Cargo.lock','crates/okx-research-worker/Cargo.toml','.cargo/config.toml','crates/okx-runtime/src/lib.rs','scripts/published-release-origin.cjs','crates/okx-research-worker/build.rs','docs/../secret.md']) assert.throws(()=>check(meta,['crates/okx-research-worker/src/ipc.rs',file]),file);
meta.resolve.nodes.find(n=>n.id==='okx-agent').deps.push({pkg:'okx-research-worker',dep_kinds:[{kind:'build'}]});
assert.throws(()=>check(meta,['crates/okx-research-worker/src/ipc.rs']));
const transitive = structuredClone(meta);
transitive.resolve.nodes.find(n=>n.id==='okx-agent').deps = [{pkg:'okx-analysis',dep_kinds:[{kind:null}]}];
transitive.resolve.nodes.find(n=>n.id==='okx-analysis').deps.push({pkg:'okx-strategy-core',dep_kinds:[{kind:'normal'}]});
assert.throws(()=>check(transitive,[pureCoreFile]),/PRODUCTION_DEPENDENCY_CHANGED/);
console.log('research release scope negative controls passed');

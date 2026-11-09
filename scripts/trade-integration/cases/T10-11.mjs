// T10-11 native component evidence: six distinct Windows OS-kill barriers.
// Full acceptance still requires all nine phases and persisted Worker causes.
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import {actualNativeWorker} from "../native-workerd.mjs";
const exec = promisify(execFile);
const marker = "T10_11_NATIVE_WINDOWS_OS_KILLS_V2:";
const recordMarker = "T10_11_NATIVE_WINDOWS_OS_KILL_RECORD_V2:";
const exactTest = "k5_windows_actual_process_kill_native_component_phases_ten_times_each";
const nativePhases = ["F0","F1","F3","F5","F6","F7"];

export async function run({caseId,root,signal,artifactsDir}) {
  assert.equal(caseId,"T10-11");
  assert.ok(artifactsDir,"retain actual OS-kill evidence");
  if(process.platform!=="win32")
    throw Error("T10_11_REQUIRES_WINDOWS_OS_PROCESS_KILL");
  const dir=resolve(artifactsDir,caseId);
  await mkdir(dir,{recursive:true});
  const {stdout:sha}=await exec("git",["rev-parse","HEAD"],{cwd:root,signal,windowsHide:true});
  // Connected Native/Worker boundaries. Do not count the following
  // 60 isolated component kills as 90 complete cross-owner proofs.
  try {
    const roots=new Set();
    for(const phase of ["F5","F0","F1","F2","F3","F4","F8"]){
    process.env.OKX_T10_11_NATIVE_CRASH=phase;
    const repetitions=[];
    for(let repetition=1;repetition<=10;repetition++){
      const started=performance.now();
      const connected=await actualNativeWorker(root,signal,{});
      assert.equal(connected.cohort_execution.reused,false,"an OS kill cannot be memoized");
      assert.equal(connected.nativeUnsent.phase,phase);
      assert.equal(connected.nativeUnsent.native_os_kills,1);
      if(["F0","F1"].includes(phase)){
        assert.equal(connected.nativeUnsent.persisted_worker.inspected.reason,"RISK_RESERVATION_NOT_FOUND");
      }else{
        assert.equal(connected.nativeUnsent.persisted_worker.inspected.reservation_state,phase==="F5"?"POSTED":"RELEASED");
      }
      const original=connected.nativeUnsent.boundary.root;
      assert.ok(!roots.has(original),"each repetition must create its own original SQL");
      roots.add(original);
      const record={source_sha:sha.trim(),phase,repetition,
        elapsed_ms:performance.now()-started,...connected};
      repetitions.push(record);
      // Preserve each completed real crash even if a later repetition fails.
      await writeFile(resolve(dir,`native-worker-${phase.toLowerCase()}-process-crash-${repetition}.json`),
        JSON.stringify(record,null,2)+"\n");
    }
    await writeFile(resolve(dir,`native-worker-${phase.toLowerCase()}-process-crashes.json`),
      JSON.stringify({schema:"okx.t10-11-native-unsent-repetitions/v1",phase,
        source_sha:sha.trim(),native_os_kills:10,full_t10_11_pass:false,
        repetitions},null,2)+"\n");
    }
  } finally {
    delete process.env.OKX_T10_11_NATIVE_CRASH;
  }
  // The invoked Rust test spawns and forcibly terminates its own genuine
  // child processes. CI precompiles before entering this case timer.
  let stdout;
  try {
    const result=await exec("cargo",[
      "test","--locked","-p","okx-agent","--lib",
      `trade_service::execute::tests::${exactTest}`,
      "--","--exact","--nocapture",
    ],{cwd:root,signal,timeout:270000,windowsHide:true,maxBuffer:8*1024*1024});
    stdout=result.stdout;
    await writeFile(resolve(dir,"native-process-kills.stdout.txt"),stdout);
    await writeFile(resolve(dir,"native-process-kills.stderr.txt"),result.stderr);
  } catch(error) {
    await writeFile(resolve(dir,"native-process-kills.stdout.txt"),error.stdout??"");
    await writeFile(resolve(dir,"native-process-kills.stderr.txt"),error.stderr??String(error));
    throw error;
  }
  assert.match(stdout,/test result: ok\./,
    "Rust Windows crash test must complete successfully");
  const lines=stdout.split(/\r?\n/)
    .filter(line=>line.includes(marker));
  assert.equal(lines.length,1,"one exact Rust 60-kill component proof required");
  const prefix=lines[0].indexOf(marker);
  const proof=JSON.parse(lines[0].slice(prefix+marker.length));
  assert.equal(proof.schema,"okx.t10-11-native-windows-os-kills/v2");
  for(const phase of nativePhases)
    assert.equal(proof[`${phase.toLowerCase()}_os_kills`],10);
  assert.equal(proof.total_os_kills,60);
  assert.equal(proof.original_wal_reopened,true);
  assert.equal(proof.unknown_never_resubmitted,true);
  assert.equal(proof.f6_independent_venue_readback,true);
  assert.equal(proof.f7_exactly_once_projection,true);
  assert.equal(proof.f3_central_release_proven,false);
  assert.equal(proof.f2_f4_f8_proven,false);
  assert.equal(proof.native_worker_same_campaign_proven,false);
  assert.equal(proof.repetitions.length,60);
  assert.ok(Number.isFinite(proof.total_ms)&&proof.total_ms>0);
  const records=stdout.split(/\r?\n/).filter(line=>line.includes(recordMarker))
    .map(line=>JSON.parse(line.slice(line.indexOf(recordMarker)+recordMarker.length)));
  assert.deepEqual(records,proof.repetitions,"each successful repetition emitted its own record");
  const identities=new Set(), stores=new Set();
  for(const row of proof.repetitions) {
    assert.ok(nativePhases.includes(row.phase));
    assert.ok(Number.isInteger(row.repetition)&&row.repetition>=1&&row.repetition<=10);
    const identity=`${row.phase}:${row.repetition}`;
    assert.ok(!identities.has(identity),`duplicate kill record ${identity}`);
    identities.add(identity);
    assert.equal(row.os_killed,true);
    assert.equal(row.central_worker_connected,false);
    assert.ok(Number.isInteger(row.process_id)&&row.process_id>0);
    assert.equal(row.venue_fixture_acceptances,row.phase==="F6"?1:0);
    assert.ok(typeof row.original_store==="string"&&row.original_store.endsWith("account.db"));
    assert.ok(!stores.has(row.original_store),"each repetition has its own original store");
    stores.add(row.original_store);
    for(const key of ["setup_ms","kill_ms","reopen_reconcile_ms","total_ms"])
      assert.ok(Number.isFinite(row[key])&&row[key]>=0,`${identity} missing ${key}`);
    assert.ok(row.total_ms>=row.setup_ms+row.kill_ms);
  }
  await writeFile(resolve(dir,"native-component-process-kills.json"),
    JSON.stringify({schema:"okx.t10-11-partial-crash-evidence/v2",
      mandatory_case_pass:false,
      source_sha:sha.trim(),
      exact_test:exactTest,
      proof,
      native_component_phases:nativePhases,
      native_component_missing_phases:["F0","F1","F2","F3","F4","F8"],
      required_repetitions_per_phase:10,
      limitations:["F0 barrier precedes prepare invocation; no in-transaction kill claimed",
        "F3 uses production SQL no-send transition; central release remains pending",
        "venue acceptance/readback is a file fixture, not an actual exchange",
        "no persisted Worker store is connected to these kills"],
      missing:["same_persisted_native_and_worker_causes",
        "zero_duplicate_finance_and_order_on_all_phases",
        "90_actual_os_process_kills_on_one_exact_sha"],
    },null,2)+"\n");
  throw Error("T10_11_ALL_NINE_NATIVE_WORKER_PHASES_TEN_TIMES_NOT_YET_PROVEN");
}

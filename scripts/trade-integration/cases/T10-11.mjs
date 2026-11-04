// T10-11: real Windows F5/F6 OS-kill prerequisite from the EXISTING Rust
// SQL owner's crash harness. No mandatory PASS until F0..F8 all execute ten
// actual child process kills and reconcile Native SQL with Worker state.
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
const exec = promisify(execFile);
const marker = "T10_11_NATIVE_WINDOWS_OS_KILLS_V1:";

export async function run({caseId,root,signal,artifactsDir}) {
  assert.equal(caseId,"T10-11");
  assert.ok(artifactsDir,"retain actual OS-kill evidence");
  if(process.platform!=="win32")
    throw Error("T10_11_REQUIRES_WINDOWS_OS_PROCESS_KILL");
  // The invoked Rust test spawns and forcibly terminates its own genuine
  // child processes. The original SQLite WAL survives and is reopened.
  const {stdout} = await exec("cargo",[
    "test","-p","okx-agent","--lib",
    "trade_service::execute::tests::k5_windows_actual_process_kill_f5_f6_ten_times_each",
    "--","--exact","--nocapture",
  ],{cwd:root,signal,timeout:270000,windowsHide:true,maxBuffer:8*1024*1024});
  assert.match(stdout,/test result: ok\./,
    "Rust Windows crash test must complete successfully");
  const lines=stdout.split(/\r?\n/)
    .filter(line=>line.includes(marker));
  assert.equal(lines.length,1,"one exact Rust 20-kill proof required");
  const prefix=lines[0].indexOf(marker);
  const proof=JSON.parse(lines[0].slice(prefix+marker.length));
  assert.equal(proof.schema,"okx.t10-11-native-windows-os-kills/v1");
  assert.equal(proof.f5_os_kills,10);
  assert.equal(proof.f6_os_kills,10);
  assert.equal(proof.total_os_kills,20);
  assert.equal(proof.original_wal_reopened,true);
  assert.equal(proof.unknown_never_resubmitted,true);
  assert.equal(proof.f6_independent_venue_readback,true);
  assert.equal(proof.f0_f4_f7_f8_proven,false);
  assert.equal(proof.native_worker_same_campaign_proven,false);
  const dir=resolve(artifactsDir,caseId);
  await mkdir(dir,{recursive:true});
  await writeFile(resolve(dir,"native-f5-f6-process-kills.json"),
    JSON.stringify({schema:"okx.t10-11-partial-crash-evidence/v1",
      mandatory_case_pass:false,
      exact_test:"k5_windows_actual_process_kill_f5_f6_ten_times_each",
      proof,
      remaining_phases:["F0","F1","F2","F3","F4","F7","F8"],
      required_repetitions_per_phase:10,
      missing:["same_persisted_native_and_worker_causes",
        "zero_duplicate_finance_and_order_on_all_phases",
        "90_actual_os_process_kills_on_one_exact_sha"],
    },null,2)+"\n");
  throw Error("T10_11_F0_F4_F7_F8_AND_NATIVE_WORKER_CAUSAL_PROOF_MISSING");
}

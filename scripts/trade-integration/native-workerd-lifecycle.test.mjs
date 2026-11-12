import {observedOwnedDescendants} from "./process-ownership.mjs";
// Whole existing Native/workerd driver, with metadata-only faults at its
// pre-handoff ownership boundary. No alternative venue or trading algorithm.
import assert from "node:assert/strict";
import {test} from "node:test";
import {execFile} from "node:child_process";
import {promisify} from "node:util";
import {mkdir, mkdtemp, readFile, rm, stat, writeFile} from "node:fs/promises";
import {createHash} from "node:crypto";
import {tmpdir} from "node:os";
import {join, resolve, sep} from "node:path";
import {fileURLToPath} from "node:url";
import {setTimeout as delay} from "node:timers/promises";
import {execOwnedChild, runScenario} from "./scenario-lifecycle.mjs";
import {nativeDriverTeardownFailure} from "./native-failure.mjs";

const invoke = promisify(execFile);
const repo = fileURLToPath(new URL("../../", import.meta.url));
const driver = join(repo, "cloudflare", "okx-mcp", "scripts", "test-runtime-do.mjs");
async function processes() {
  if (process.platform === "win32") {
    const {stdout} = await invoke("powershell.exe", ["-NoProfile", "-NonInteractive", "-Command",
      "Get-CimInstance Win32_Process | Select-Object ProcessId,ParentProcessId,Name,@{Name='created';Expression={if ($_.CreationDate) {$_.CreationDate.ToUniversalTime().ToString('o')} else {$null}}} | ConvertTo-Json -Compress"],
    {windowsHide:true, timeout:10000, maxBuffer:1024*1024});
    const values = JSON.parse(stdout);
    return (Array.isArray(values) ? values : [values]).map(row => ({
      pid:row.ProcessId, parent_pid:row.ParentProcessId, name:row.Name, created:row.created,
    }));
  }
  const {stdout} = await invoke("ps", ["-eo", "pid=,ppid=,lstart=,comm="],
    {timeout:10000, maxBuffer:1024*1024});
  return stdout.trim().split(/\r?\n/).map(line => {
    const [pid, parent, ...fields] = line.trim().split(/\s+/);
    return {pid:Number(pid), parent_pid:Number(parent),
      created:fields.slice(0,5).join(" "), name:fields.slice(5).join(" ")};
  });
}
const sameProcess = (actual, saved) => actual.pid === saved.pid &&
  actual.created === saved.created && actual.name === saved.name;
async function receipts(path) {
  try {
    const text = await readFile(path, "utf8");
    assert.ok(text.length < 128*1024, "lifecycle metadata must be bounded");
    // A final append may still be in flight; only consume complete lines.
    return text.split("\n").slice(0,-1).map(line => JSON.parse(line));
  } catch (error) { if (error.code === "ENOENT") return []; throw error; }
}
async function waitForStage(run, stage) {
  const until = Date.now()+150000;
  while (Date.now()<until) {
    const rows = await receipts(run.path);
    const found = rows.find(row => row.stage === stage);
    if (found) return {rows, found};
    if (run.finished) {
      const finalRows = await receipts(run.path);
      const finalStage = finalRows.find(row => row.stage === stage);
      if (finalStage) return {rows:finalRows, found:finalStage};
      const error = Error("REAL_DRIVER_EXITED_BEFORE_"+stage.toUpperCase());
      error.cause = run.failure;
      throw error;
    }
    await delay(25);
  }
  throw Error("REAL_DRIVER_LIFECYCLE_STAGE_DEADLINE");
}
function startRun(root, name, outcome) {
  const path = join(root, name+".jsonl");
  const controller = new AbortController();
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
    !/^OKX_(?:T09_|T10_|B1_|S2_|INTEGRATION_HANDOFF_PATH)/.test(key)));
  env.OKX_NATIVE_WORKER_INTEGRATION = "1";
  const run = {path, controller, finished:false, failure:null, output:null};
  run.completion = runScenario(signal => execOwnedChild(process.execPath,
    [driver, "--lifecycle-probe", JSON.stringify({outcome, receipt_path:path})],
    {cwd:repo, env, signal:AbortSignal.any([signal, controller.signal]),
      maxBuffer:12*1024*1024, windowsHide:true}),
  {deadline:160000}).then(output => {run.output=output;}, error => {run.failure=error;})
    .finally(() => {run.finished=true;});
  return run;
}
async function noOwnedSurvivors(ready) {
  const deadline = Date.now() + 10000;
  for (;;) {
    const now = await processes();
    const survivors = ready.owned_descendants.filter(saved =>
      now.some(actual => sameProcess(actual, saved)));
    if (!survivors.length) return;
    assert.ok(Date.now() < deadline,
      "original owned Native/workerd/service processes survived driver closure: " +
      JSON.stringify(survivors));
    await delay(25);
  }
}
async function joinedDriver(run) {
  let timer;
  try {
    await Promise.race([run.completion, new Promise((_, reject) => {
      timer = setTimeout(() => reject(Error("LIFECYCLE_DRIVER_JOIN_UNPROVEN")), 10000);
    })]);
  } finally { clearTimeout(timer); }
}
async function finishOwned(run) {
  // This cleanup must not conceal a RED assertion. The driver is always
  // started through the existing process owner; orphan identities are those
  // observed before this unique driver's handoff, never name-based guesses.
  const rows = await receipts(run.path);
  const ready = rows.find(row => row.stage === "worker_and_native_ready");
  run.controller.abort();
  await run.completion;
  if (ready) {
    for (const saved of ready.owned_descendants) {
      const actual = (await processes()).find(row => sameProcess(row, saved));
      if (!actual) continue; // PID reused or original process already exited.
      try {
        if (process.platform === "win32")
          await invoke("taskkill.exe", ["/PID", String(saved.pid), "/T", "/F"],
            {windowsHide:true, timeout:5000});
        else process.kill(saved.pid, "SIGKILL");
      } catch (error) {
        if ((await processes()).some(row => sameProcess(row, saved))) throw error;
      }
    }
  }
  if (ready) await noOwnedSurvivors(ready);
}


// A recycled Windows parent PID is not proof of process ownership.
test("lifecycle ownership rejects recycled PID without hiding real children", () => {
  const rows = [
    {pid:100,name:"driver.exe",parent_pid:1,created:"2026-11-12T13:05:00.000Z"},
    {pid:101,name:"csrss.exe",parent_pid:100,created:"2026-11-12T13:01:00.000Z"},
    {pid:102,name:"winlogon.exe",parent_pid:101,created:"2026-11-12T13:01:01.000Z"},
    {pid:103,name:"vctip.exe",parent_pid:100,created:"2026-11-12T13:04:59.000Z"},
    {pid:104,name:"okx-agent.exe",parent_pid:100,created:"2026-11-12T13:05:01.000Z"},
    {pid:105,name:"workerd.exe",parent_pid:104,created:"2026-11-12T13:05:02.000Z"},
    {pid:106,name:"child.exe",parent_pid:105,created:"2026-11-12T13:05:03.000Z"},
    {pid:107,name:"stale-service.exe",parent_pid:105,created:"2026-11-12T13:04:00.000Z"},
  ];
  assert.deepEqual(
    observedOwnedDescendants(rows,100).map(row=>row.pid),
    [104,105,106],
    "Only causally newer descendants may be asserted or terminated",
  );
  assert.throws(
    ()=>observedOwnedDescendants(rows,999),
    /LIFECYCLE_ROOT_IDENTITY_UNAVAILABLE/,
    "Missing owner identity cannot prove a process tree",
  );
  assert.throws(
    ()=>observedOwnedDescendants([{...rows[0],created:null},rows[4]],100),
    /LIFECYCLE_PROCESS_IDENTITY_UNAVAILABLE/,
    "Missing owner creation time must fail closed",
  );
});

for (const outcome of ["success", "error", "timeout", "error_teardown"]) {
  test(`actual Native/workerd ${outcome} closes owned processes and isolates next run`,
    {skip:process.platform !== "win32", timeout:350000}, async () => {
    const artifactBase = process.env.OKX_LIFECYCLE_ARTIFACTS;
    if (artifactBase) await mkdir(resolve(artifactBase), {recursive:true});
    const root = await mkdtemp(join(artifactBase ? resolve(artifactBase) : tmpdir(),
      "okx-native-worker-lifecycle-"+outcome+"-"));
    await writeFile(join(root,"manifest.json"), JSON.stringify({
      schema:"okx.native-worker-lifecycle-manifest/v1", outcome,
      test_file_sha256:createHash("sha256").update(await readFile(fileURLToPath(import.meta.url))).digest("hex"),
      driver_file_sha256:createHash("sha256").update(await readFile(driver)).digest("hex"),
      source_identity:"CONTENT_HASHES_ONLY_NO_COMMIT_CLAIM",
    })+"\n");
    const runs = [];
    let accepted = false;
    let primary = null;
    try {
      const run = startRun(root, outcome, outcome);
      runs.push(run);
      const {found:ready} = await waitForStage(run, "worker_and_native_ready");
      assert.equal(ready.schema, "okx.native-worker-lifecycle/v1");
      assert.equal(ready.native.accounts.length, 2);
      assert.ok(ready.owned_descendants.some(row => row.pid === ready.native.native_pid));
      assert.ok(ready.owned_descendants.some(row => /workerd(?:\.exe)?$/i.test(row.name)));
      assert.ok(ready.owned_descendants.every(row => row.created));
      const {rows, found:result} = await waitForStage(run, "driver_result");
      if (outcome === "success") {
        assert.equal(result.outcome, "success");
        const teardown = rows.find(row => row.stage === "native_completed").native_teardown;
        assert.equal(teardown.native_pid, ready.native.native_pid);
        assert.equal(teardown.native_wss_joined, true);
        assert.equal(teardown.public_ws_joined, true);
        assert.equal(teardown.account_sources_shutdown_complete, true);
        assert.equal(teardown.sql_owners_terminated, 2);
      } else {
        const code = outcome === "timeout" ? "LIFECYCLE_PROBE_PRIMARY_TIMEOUT" : "LIFECYCLE_PROBE_PRIMARY_ERROR";
        assert.equal(rows.find(row => row.stage === "primary_failure").reason_code, code);
        assert.equal(result.outcome, "error");
        assert.equal(result.primary_error, code);
        assert.deepEqual(result.teardown_errors,
          outcome === "error_teardown" ? ["LIFECYCLE_PROBE_POST_DISPOSE_INJECTED_ERROR"] : []);
        assert.equal(result.reported_error, code, "cleanup must retain the primary failure");
        if (outcome === "error_teardown") {
          assert.equal(rows.find(row => row.stage === "cleanup_fault").second_dispose_completed, true);
          assert.ok(rows.some(row => row.stage === "miniflare_disposed" && row.owner === "second"));
        }
      }
      assert.ok(rows.some(row => row.stage === "miniflare_disposed" && row.owner === "second"));
      assert.ok(rows.some(row => row.stage === "runtime_root_removed"));
      // A reported outcome must not certify live owned descendants. The
      // ordinary-error regression already passed on the old finally; only
      // observed failures justify changing its resource ownership.
      await joinedDriver(run);
      await noOwnedSurvivors(ready);
      const closure = rows.find(row => row.stage === "native_owner_closed");
      assert.ok(closure, "Native owner must be joined before reporting driver completion");
      if (outcome !== "success") {
        assert.equal(closure.closure, "FORCED_OS_PROCESS_TREE_TERMINATION");
        assert.equal(closure.graceful_task_sql_join_receipt, false);
      }
      if (outcome === "success") assert.equal(run.failure, null);
      else assert.ok(run.failure);
      if (outcome === "error_teardown") {
        const teardown = nativeDriverTeardownFailure(run.failure);
        assert.equal(teardown.parse_state, "VALID");
        assert.equal(teardown.primary_code, "LIFECYCLE_PROBE_PRIMARY_ERROR");
        assert.deepEqual(teardown.failures,
          [{owner:"second", code:"LIFECYCLE_PROBE_POST_DISPOSE_INJECTED_ERROR"}]);
        assert.equal(teardown.owners_closed, true);
        assert.equal(teardown.root_retained, false);
      }
      await assert.rejects(stat(ready.runtime_root), {code:"ENOENT"});
      const saved = await readFile(run.path, "utf8");

      const next = startRun(root, outcome+"-next", "success");
      runs.push(next);
      const nextReady = (await waitForStage(next, "worker_and_native_ready")).found;
      await waitForStage(next, "driver_result");
      await joinedDriver(next);
      assert.equal(next.failure, null);
      await noOwnedSurvivors(nextReady);
      assert.notEqual(nextReady.runtime_root, ready.runtime_root);
      const originalNative = ready.owned_descendants.find(row => row.pid === ready.native.native_pid);
      const nextNative = nextReady.owned_descendants.find(row => row.pid === nextReady.native.native_pid);
      assert.equal(sameProcess(originalNative, nextNative), false);
      assert.equal(await readFile(run.path, "utf8"), saved, "next run must preserve prior stage evidence");
      accepted = true;
    } catch (error) {
      primary = error;
      throw error;
    } finally {
      const cleanup = [];
      for (const run of runs) {
        try { await finishOwned(run); } catch (error) { cleanup.push(error); }
      }
      if (!artifactBase) assert.ok(resolve(root).startsWith(resolve(tmpdir())+sep));
      if (accepted && !cleanup.length && !artifactBase) await rm(root, {recursive:true, force:true});
      else process.stderr.write("NATIVE_WORKER_LIFECYCLE_EVIDENCE_V1:"+
        JSON.stringify({receipt_root:root, outcome})+"\n");
      if (cleanup.length) throw new AggregateError([
        ...(primary ? [primary] : []), ...cleanup,
      ], primary?.message ?? "LIFECYCLE_TEST_OWNED_CLEANUP_FAILED", {cause:primary ?? cleanup[0]});
    }
  });
}

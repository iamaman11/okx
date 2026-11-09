import assert from "node:assert/strict";
import { test } from "node:test";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve, sep } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { execOwnedChild, preserveTeardownFailure, runScenario } from "./scenario-lifecycle.mjs";

const alive = pid => {
  try { process.kill(pid, 0); return true; }
  catch (error) { if (error.code === "ESRCH") return false; throw error; }
};
async function finishFixture(path) {
  let evidence;
  try { evidence = JSON.parse(await readFile(path, "utf8")); }
  catch (error) { if (error.code === "ENOENT") return; throw error; }
  // These PIDs were emitted by this unique fixture, not process-name matches.
  for (const pid of [evidence.pid, evidence.nested]) {
    if (!Number.isSafeInteger(pid) || pid <= 0 || !alive(pid)) continue;
    await promisify(execFile)("taskkill.exe", ["/PID", String(pid), "/T", "/F"],
      { windowsHide:true, timeout:5000 });
  }
  for (const pid of [evidence.pid, evidence.nested]) {
    if (!Number.isSafeInteger(pid) || pid <= 0) continue;
    for (let i=0; i<50 && alive(pid); i++) await delay(20);
    assert.equal(alive(pid), false, "test-owned process must exit even after an assertion fails");
  }
}
const child = String.raw`
  const fs = require('node:fs');
  const { spawn } = require('node:child_process');
  const [mode, path] = process.argv.slice(1);
  let nested;
  if (mode === 'timeout') nested = spawn(process.execPath,
    ['-e', 'setInterval(() => {}, 1000)'], { stdio:'ignore', windowsHide:true });
  fs.writeFileSync(path, JSON.stringify({ pid:process.pid, nested:nested?.pid, stage:'entered' }));
  if (mode === 'failure') { process.stderr.write('FIRST_SCENARIO_FAILURE'); process.exitCode=7; }
  else if (mode === 'timeout') setInterval(() => {}, 1000);
  else process.stdout.write('COMPLETED');
`;

for (const mode of ["success", "failure", "timeout"]) {
  test(`Windows scenario ${mode} joins child, retains evidence and isolates next run`,
    { skip:process.platform !== "win32" }, async () => {
    const root = await mkdtemp(join(tmpdir(), "okx-lifecycle-"));
    const artifact = join(root, "stage.json");
    try {
      let failed;
      let output;
      try {
        output = await runScenario(signal => execOwnedChild(process.execPath,
          ["-e", child, mode, artifact], { signal, maxBuffer: 1024 * 1024 }),
        { deadline: mode === "timeout" ? 1000 : 5000 });
      } catch (error) { failed = error; }
      const evidence = JSON.parse(await readFile(artifact, "utf8"));
      assert.equal(evidence.stage, "entered");
      assert.equal(alive(evidence.pid), false, "direct child is joined before return");
      if (evidence.nested) {
        for (let i = 0; i < 50 && alive(evidence.nested); i++) await delay(20);
        assert.equal(alive(evidence.nested), false, "owned descendant must exit too");
      }
      if (mode === "success") {
        assert.equal(failed, undefined);
        assert.equal(output.stdout, "COMPLETED");
      } else {
        assert.ok(failed, "expected scenario failure must not be rewritten to success");
        if (mode === "failure") {
          assert.equal(failed.code, 7);
          assert.equal(failed.stderr, "FIRST_SCENARIO_FAILURE");
        } else assert.equal(failed.message, "CASE_DEADLINE_EXCEEDED");
      }
      const next = await runScenario(signal => execOwnedChild(process.execPath,
        ["-e", child, "success", join(root, "next.json")], { signal }), { deadline: 5000 });
      assert.equal(next.stdout, "COMPLETED");
      const subsequent = JSON.parse(await readFile(join(root, "next.json"), "utf8"));
      assert.notEqual(subsequent.pid, evidence.pid);
      assert.deepEqual(JSON.parse(await readFile(artifact, "utf8")), evidence);
    } finally {
      await finishFixture(artifact);
      await finishFixture(join(root, "next.json"));
      assert.ok(resolve(root).startsWith(resolve(tmpdir()) + sep));
      await rm(root, { recursive:true, force:true });
    }
  });
}

test("timeout joins async finally before caller can restore its environment", async () => {
  let joined = false;
  await assert.rejects(runScenario(async signal => {
    try { await new Promise(resolve => signal.addEventListener("abort", resolve, { once:true })); }
    finally { await delay(30); joined = true; }
  }, { deadline: 10 }), /CASE_DEADLINE_EXCEEDED/);
  assert.equal(joined, true);
});

test("already aborted child is not started", async () => {
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(execOwnedChild(process.execPath, ["-e", "throw Error('must not start')"],
    { signal:controller.signal }), { name:"AbortError" });
});

test("nested diagnostic formatting preserves unjoined child failure identity", () => {
  for (const code of ["CHILD_TEARDOWN_TIMEOUT", "CHILD_JOIN_TIMEOUT", "CHILD_TREE_TERMINATION_FAILED"]) {
    const original = Error(code);
    assert.throws(() => preserveTeardownFailure(original), error =>
      error === original && error.teardownFailure && error.teardownReason === code);
  }
  const aggregate = new AggregateError([Error("CASE_DEADLINE_EXCEEDED"), Error("SCENARIO_TEARDOWN_TIMEOUT")]);
  aggregate.teardownFailure = true;
  assert.throws(() => preserveTeardownFailure(aggregate), error => error === aggregate);
  assert.equal(preserveTeardownFailure(Error("ORDINARY_ASSERTION")), undefined);
});

test("timeout retains a later aggregate teardown failure rather than admitting another case", async () => {
  const teardown = new AggregateError([Error("NESTED_CLEANUP_FAILED")], "NESTED_OPERATION_FAILED");
  teardown.teardownFailure = true;
  await assert.rejects(runScenario(async signal => {
    await new Promise(resolve => signal.addEventListener("abort", resolve, { once:true }));
    throw teardown;
  }, { deadline:10 }), error => error.message === "CASE_DEADLINE_EXCEEDED"
    && error.teardownFailure === true && error.errors[1] === teardown);
});

test("Windows exited launcher cannot certify orphan ownership; test retains that ownership explicitly",
  { skip:process.platform !== "win32" }, async () => {
  const root = await mkdtemp(join(tmpdir(), "okx-lifecycle-orphan-"));
  const path = join(root, "owned.json");
  let nested;
  try {
    let failure;
    try {
      await runScenario(signal => execOwnedChild(process.execPath, ["-e", String.raw`
        const {spawn} = require('node:child_process');
        const fs = require('node:fs');
        const nested = spawn(process.execPath, ['-e','setInterval(() => {},1000)'],
          {stdio:['ignore',process.stdout,process.stderr],windowsHide:true});
        fs.writeFileSync(process.argv[1], JSON.stringify({pid:process.pid,nested:nested.pid}));
        nested.unref();
      `, path], { signal }), { deadline:1000 });
    } catch (error) { failure = error; }
    const evidence = JSON.parse(await readFile(path, "utf8"));
    nested = evidence.nested;
    if (process.platform === "win32") {
      // Windows may close the launcher's pipes despite a surviving descendant.
      // This characterizes the generic exec boundary, NOT S1-05 acceptance.
      if (failure) {
        assert.equal(failure.message, "CASE_DEADLINE_EXCEEDED");
        assert.equal(failure.teardownFailure, true);
        assert.equal(failure.errors[1].message, "CHILD_TREE_TERMINATION_FAILED");
      } else assert.equal(alive(evidence.pid), false,
        "normal completion certifies only the launched child's close");
    } else {
      assert.ok(failure, "timeout is not a completed scenario");
      assert.equal(failure.message, "CASE_DEADLINE_EXCEEDED");
      assert.equal(alive(nested), false);
    }
  } finally {
    // The test recorded this descendant's ownership before its leader exited;
    // the generic launcher cannot invent that evidence, so it reports failure.
    if (nested && alive(nested)) {
      if (process.platform === "win32")
        await promisify(execFile)("taskkill.exe", ["/PID",String(nested),"/T","/F"],
          {windowsHide:true,timeout:5000});
      else process.kill(nested,"SIGKILL");
    }
    if (nested) {
      for (let i = 0; i < 50 && alive(nested); i++) await delay(20);
      assert.equal(alive(nested), false, "test-owned descendant is never leaked");
    }
    assert.ok(resolve(root).startsWith(resolve(tmpdir()) + sep));
    await rm(root, { recursive:true, force:true });
  }
});

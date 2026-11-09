// Existing acceptance-runner lifecycle boundary; no trading or fixture policy.
import { execFile } from "node:child_process";

const cleanupMs = 10_000;
const unjoined = error => error?.teardownFailure ||
  /^CHILD_(TEARDOWN_TIMEOUT|JOIN_TIMEOUT|TREE_TERMINATION_FAILED)$/.test(error?.message ?? "");
// Diagnostic formatting must not erase an unjoined-child outcome. The runner
// relies on this exact error to stop before another scenario can be admitted.
export function preserveTeardownFailure(error) {
  if (unjoined(error)) {
    error.teardownFailure = true;
    error.teardownReason ??= error.message;
    throw error;
  }
}
async function bounded(promise, milliseconds, code) {
  let timer;
  try {
    return await Promise.race([promise, new Promise((_, reject) => {
      timer = setTimeout(() => reject(Error(code)), milliseconds);
    })]);
  } finally { clearTimeout(timer); }
}

// On timeout, joining the aborted operation must precede restoring case-scoped
// environment or starting another case. Keep the original outcome as primary.
export async function runScenario(run, { deadline, timeoutCode = "CASE_DEADLINE_EXCEEDED" }) {
  const controller = new AbortController();
  let timer;
  const operation = Promise.resolve().then(() => run(controller.signal));
  try {
    return await Promise.race([operation, new Promise((_, reject) => {
      timer = setTimeout(() => {
        controller.abort();
        reject(Error(timeoutCode));
      }, deadline);
    })]);
  } catch (primary) {
    controller.abort();
    if (unjoined(primary)) {
      primary.teardownFailure = true;
      primary.teardownReason = primary.message;
    }
    try {
      const error = await bounded(operation.then(() => null, error => error),
        cleanupMs, "SCENARIO_TEARDOWN_TIMEOUT");
      if (error !== primary && unjoined(error))
        throw error;
    }
    catch (teardown) {
      const failure = new AggregateError([primary, teardown], primary.message, { cause: primary });
      failure.teardownFailure = true;
      throw failure;
    }
    throw primary;
  } finally { clearTimeout(timer); }
}

// execFile's AbortError can arrive before close and only terminates its direct
// child. The native/Worker launcher must stay alive until its children finish.
// A failed tree termination is explicitly unproven; callers must stop the matrix.
// Windows taskkill cannot recover ownership of descendants of an exited leader.
export async function execOwnedChild(file, args, options = {}) {
  const { signal, timeout, ...spawnOptions } = options;
  signal?.throwIfAborted();
  let child;
  let stopReason;
  let stopping;
  let timer;
  let result;
  let didStop;
  const stopped = new Promise(resolve => { didStop = resolve; });
  const completed = new Promise(resolve => {
    child = execFile(file, args, {
      ...spawnOptions, windowsHide: true, detached: process.platform !== "win32",
    }, (error, stdout, stderr) => { result = { error, stdout, stderr }; });
    child.once("close", () => resolve());
  });
  const stop = reason => {
    if (stopping || !child.pid) return;
    stopReason = reason;
    stopping = process.platform === "win32"
      ? new Promise((resolve, reject) => {
        execFile("taskkill.exe", ["/PID", String(child.pid), "/T", "/F"],
          { windowsHide: true, timeout: 5000 }, error => {
            // A failed tree termination cannot prove that descendants exited,
            // even if the direct child happened to finish concurrently.
            if (error) reject(error);
            else resolve();
          });
      })
      : Promise.resolve().then(() => {
        try { process.kill(-child.pid, "SIGKILL"); }
        catch (error) { if (error.code !== "ESRCH") throw error; }
      });
    // Handled here until completion below attaches the authoritative await.
    stopping.catch(() => {});
    didStop();
  };
  const abort = () => stop(Object.assign(Error("CHILD_ABORTED"), { code: "ABORT_ERR" }));
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) abort();
  if (timeout) timer = setTimeout(() => stop(Object.assign(Error("CHILD_DEADLINE_EXCEEDED"),
    { code: "ETIMEDOUT" })), timeout);
  try {
    await Promise.race([completed, stopped.then(async () => {
      try { await bounded(stopping, cleanupMs, "CHILD_TEARDOWN_TIMEOUT"); }
      catch (cause) { throw Error("CHILD_TREE_TERMINATION_FAILED", { cause }); }
      await bounded(completed, cleanupMs, "CHILD_JOIN_TIMEOUT");
    })]);
    if (stopping) {
      try { await bounded(stopping, cleanupMs, "CHILD_TEARDOWN_TIMEOUT"); }
      catch (cause) { throw Error("CHILD_TREE_TERMINATION_FAILED", { cause }); }
    }
    const { error, stdout, stderr } = result;
    const failure = stopReason ?? error;
    if (failure) throw Object.assign(failure, { stdout, stderr, pid: child.pid });
    return { stdout, stderr };
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", abort);
    // Do not retain the dead leader's pipes indefinitely after a reported
    // termination failure. This releases readers; it is not proof of tree exit.
    if (stopping && result === undefined) {
      child.stdout?.destroy();
      child.stderr?.destroy();
    }
  }
}

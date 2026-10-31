#!/usr/bin/env node
// Workerd/Miniflare executes the ACTUAL RuntimeSession with SQLite-backed DO
// transactions. Only the incoming WSS frames are scripted. No Map storage,
// alternate RiskBook, exchange server, endpoint in production, or financial stub.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { Miniflare } from "miniflare";

const root = await mkdtemp(join(tmpdir(), "okx-workerd-risk-"));
const outfile = join(root, "runtime-test.mjs");
const FRAME_SCHEMA = "okx.direct-transport.frame/v2";
const configuration = {
  schema: "okx.governance/v3",
  revision: 0,
  installation_id: "installation_test",
  campaign_id: "sha256:" + "c".repeat(64),
  campaign_loss_limit_usd: "10",
  campaign_entry_limit_usd: "10",
  max_total_modelled_loss_usdt: "5",
  accounts: [],
};
const token = "integration-only-ephemeral-token";
const tokenHash = createHash("sha256").update(token).digest("hex");
// Miniflare 5 uses the build-output worker manifest and typed bindings.
const makeOptions = (bundleContents) => ({
  resourcePersistencePath: join(root, "sqlite-do"),
  workers: [{
    config: {
      name: "okx-test-runtime",
      compatibilityDate: "2026-09-30",
      compatibilityFlags: ["global_fetch_strictly_public"],
      manifest: {
        mainModule: "runtime-test.mjs",
        modulesRoot: root,
        modules: {
          "runtime-test.mjs": { type: "esm", contents: bundleContents },
        },
      },
      env: {
        RUNTIME: {
          type: "durable-object",
          worker: "okx-test-runtime",
          exportName: "RuntimeSession",
        },
        RUNTIME_TOKEN_SHA256: { type: "text", value: tokenHash },
      },
      exports: {
        RuntimeSession: { type: "durable-object", storage: "sqlite" },
      },
    },
  }],
});
const installation = {
  contract: FRAME_SCHEMA,
  release_sha256: "f".repeat(64),
  operation_id: "install_integration_001",
  configuration,
};

async function request(mf, pathname, method = "GET", body) {
  const response = await mf.dispatchFetch("http://localhost" + pathname, {
    method,
    ...(body === undefined ? {} : {
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    }),
  });
  return { status: response.status, body: await response.json() };
}
function frames(ws) {
  const received = [];
  const waiters = [];
  const onMessage = (event) => {
    const frame = JSON.parse(event.data);
    const pending = waiters.find((w) => w.match(frame));
    if (pending) {
      waiters.splice(waiters.indexOf(pending), 1);
      clearTimeout(pending.timer);
      pending.resolve(frame);
    } else {
      received.push(frame);
    }
  };
  ws.addEventListener("message", onMessage);
  ws.accept();
  return {
    send(frame) { ws.send(JSON.stringify(frame)); },
    next(match) {
      const idx = received.findIndex(match);
      if (idx >= 0) return Promise.resolve(received.splice(idx, 1)[0]);
      return new Promise((resolve, reject) => {
        const waiter = { match, resolve, timer: null };
        waiter.timer = setTimeout(() => {
          const pos = waiters.indexOf(waiter);
          if (pos >= 0) waiters.splice(pos, 1);
          reject(new Error("WORKER_FRAME_TIMEOUT"));
        }, 5000);
        waiters.push(waiter);
      });
    },
    close() { ws.close(1000, "integration complete"); },
  };
}
async function session(mf) {
  const reply = await mf.dispatchFetch("http://localhost/runtime", {
    headers: { upgrade: "websocket", authorization: "Bearer " + token },
  });
  assert.equal(reply.status, 101, "auth-bound real runtime WSS handshake");
  assert.ok(reply.webSocket, "WebSocketPair exists only through production RuntimeSession");
  const ws = frames(reply.webSocket);
  ws.send({
    type: "hello",
    schema: FRAME_SCHEMA,
    runtime_id: "windows-primary",
    connection_id: "native_" + "a".repeat(32),
    installation_id: configuration.installation_id,
    contract: "okx.product/v1",
    runtime_profile: "engine",
    agent_pid: 12345,
    agent_binary_sha256: "b".repeat(64),
    agent_incarnation: "ctl_" + "d".repeat(32),
  });
  const hello = await ws.next((f) => f.type === "hello_ack");
  assert.ok(hello.connection_generation > 0);
  assert.ok(hello.session_id.startsWith("session_"));
  const policy = await ws.next((f) => f.type === "policy_snapshot");
  assert.equal(policy.connection_generation, hello.connection_generation);
  assert.equal(policy.snapshot.state.installation_id, configuration.installation_id);
  return {ws,hello};
}
async function summary({ws,hello}, commandId) {
  ws.send({
    type: "risk_control",
    schema: FRAME_SCHEMA,
    session_id: hello.session_id,
    connection_generation: hello.connection_generation,
    command: {
      schema: "okx.risk-command/v1",
      command_id: commandId,
      installation_id: configuration.installation_id,
      action: { action: "SUMMARY" },
    },
  });
  const frame = await ws.next((f) => f.type === "risk_control_result" && f.result?.command_id === commandId);
  assert.equal(frame.connection_generation, hello.connection_generation);
  assert.equal(frame.result.status, "OBSERVED");
  assert.equal(frame.result.budget?.campaign_limit_usd, "10");
  assert.equal(frame.result.budget?.entry_budget, null);
  return frame.result;
}
let first, second;
try {
  await build({
    entryPoints: [fileURLToPath(new URL("../src/integration/runtime_entry.ts", import.meta.url))],
    outfile, bundle: true, write: true, format: "esm",
    platform: "browser", target: "es2022", logLevel: "warning",
  });
  const opts = makeOptions(await readFile(outfile, "utf8"));
  first = new Miniflare(opts);
  const install = await request(first, "/installation/initialize", "POST", installation);
  assert.equal(install.status, 200, "real governance/RiskBook created in one DO transaction");
  const socket1 = await session(first);
  await summary(socket1, "risk_workerd_first_0001");
  socket1.ws.close();
  await first.dispose();
  first = null;
  second = new Miniflare(opts);
  const replay = await request(second, "/installation/initialize", "POST", installation);
  assert.equal(replay.status, 200, "persisted installation receipt is idempotent after restart");
  const persisted = await request(second, "/account-governance");
  assert.equal(persisted.status, 200);
  assert.equal(persisted.body.installation_id, configuration.installation_id);
  const socket2 = await session(second);
  assert.ok(socket2.hello.connection_generation > socket1.hello.connection_generation,
    "restarted DO retains connection generation");
  await summary(socket2, "risk_workerd_second_0002");
  socket2.ws.close();
  console.log(JSON.stringify({
    suite:"runtime-session-sqlite-do",
    status:"PASS",
    real_durable_object: true,
    persisted_generation: true,
    risk_book_source: "RuntimeSession.webSocketMessage",
    risk_monetary_post_count: 0,
  }));
} finally {
  if (first) await first.dispose();
  if (second) await second.dispose();
  await rm(root, {recursive: true, force: true});
}

// Hermetic Miniflare entrypoint only. Never imported by index.ts or Wrangler.
// WSS admissions use the exact production Worker prefilter and Durable Object.
import {defaultHandler} from "../auth.js";
import {RuntimeSession} from "../runtime_session.js";
import {runtimeStub, type Env} from "../shared.js";

export {RuntimeSession};

export default {
  async fetch(request: Request, env: Env & {CONTROL_ATTEST_E2E_LOCAL_ONLY?: string}): Promise<Response> {
    const url = new URL(request.url);
    if (url.hostname !== "localhost" || env.CONTROL_ATTEST_E2E_LOCAL_ONLY !== "1")
      return new Response("not found", {status: 404});
    const path = url.pathname;
    if (path === "/runtime" || path === "/attestor")
      return defaultHandler.fetch(request, env);
    // Read-only DO diagnostics are an internal call, not a public Worker API.
    if (path === "/attestation/research" && request.method === "GET")
      return (await runtimeStub(env)).fetch(request);
    return new Response("not found", {status: 404});
  },
};

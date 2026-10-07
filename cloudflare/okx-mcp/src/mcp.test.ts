import { mcpApi } from "./mcp.js";
import type { Env } from "./shared.js";

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(message);
}

const request = new Request("https://okx.invalid/mcp", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({
    jsonrpc: "2.0",
    id: 1,
    method: "tools/list",
    params: {},
  }),
});

const response = await mcpApi.fetch(
  request,
  {} as Env,
  { auth: { scope: ["mcp:use"] } },
);
assert(response.ok, "tools/list must remain callable in the contract fixture");

const body = await response.text();
const bytes = new TextEncoder().encode(body).byteLength;
assert(
  bytes <= 24 * 1024,
  `tools/list exceeds 24 KiB context budget: ${bytes} bytes`,
);

const rpc = JSON.parse(body) as {
  result?: { tools?: unknown[] };
};
const tools = rpc.result?.tools;
assert(Array.isArray(tools), "tools/list must return a tools array");
assert(
  tools.length <= 16,
  `coarse MCP surface exceeded 16-tool budget: ${tools.length} tools`,
);

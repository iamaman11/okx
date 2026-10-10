import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const output = mkdtempSync(join(tmpdir(), "okx-mcp-tool-result-"));
const tsc = resolve("node_modules/typescript/bin/tsc");

const mcpSource = readFileSync(
  new URL("../src/mcp.ts", import.meta.url),
  "utf8",
);
const toolsListStart = mcpSource.indexOf('if (rpc.method === "tools/list")');
const toolsCallStart = mcpSource.indexOf('if (rpc.method !== "tools/call"', toolsListStart);
if (toolsListStart < 0 || toolsCallStart <= toolsListStart) {
  throw new Error("unable to locate the owned tools/list schema block");
}
const toolsListBlock = mcpSource.slice(toolsListStart, toolsCallStart);
const toolsListBytes = new TextEncoder().encode(toolsListBlock).byteLength;
if (toolsListBytes > 24 * 1024) {
  throw new Error(`tools/list source schema exceeds 24 KiB budget: ${toolsListBytes} bytes`);
}
const toolCount = (toolsListBlock.match(/\n\s+name: "/g) ?? []).length
  + (toolsListBlock.includes("...EXECUTION_TOOLS") ? 2 : 0);
if (toolCount > 16) {
  throw new Error(`coarse MCP surface exceeded 16-tool budget: ${toolCount} tools`);
}

try {
  execFileSync(
    process.execPath,
    [tsc, "--rootDir", "src", "--outDir", output, "--noEmit", "false"],
    { stdio: "inherit" },
  );
  for (const file of ["shared.test.js", "execution_tools.test.js"]) {
    execFileSync(process.execPath, [join(output, file)], {
      stdio: "inherit",
    });
  }
} finally {
  rmSync(output, { recursive: true, force: true });
}

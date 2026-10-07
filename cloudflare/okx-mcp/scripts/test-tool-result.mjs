import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const output = mkdtempSync(join(tmpdir(), "okx-mcp-tool-result-"));
const tsc = resolve("node_modules/typescript/bin/tsc");

try {
  execFileSync(
    process.execPath,
    [tsc, "--outDir", output, "--noEmit", "false"],
    { stdio: "inherit" },
  );
  execFileSync(process.execPath, [join(output, "shared.test.js")], {
    stdio: "inherit",
  });
} finally {
  rmSync(output, { recursive: true, force: true });
}

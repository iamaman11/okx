import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, mkdirSync, writeFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const output = mkdtempSync(join(tmpdir(), "okx-mcp-tool-result-"));
const tsc = resolve("node_modules/typescript/bin/tsc");


try {
  execFileSync(
    process.execPath,
    [tsc, "--rootDir", "src", "--outDir", output, "--noEmit", "false"],
    { stdio: "inherit" },
  );
  let executed=0;
  for (const file of ["auth.test.js", "shared.test.js", "execution_tools.test.js", "risk_book.test.js", "account_governance.test.js", "account_governance_store.test.js", "risk_authority.test.js", "runtime_session.test.js", "runtime_backpressure.test.js", "mcp.test.js", "acceptance_composed.test.js"]) {
    execFileSync(process.execPath, [join(output, file)], {
      stdio: "inherit",
    });
    executed++;
  }
  const {PUBLIC_TOOLS}=await import(pathToFileURL(join(output,"execution_tools.js")));
  const {payloadFixtures}=await import(pathToFileURL(join(output,"execution_tools.test.js")));
  mkdirSync("contracts",{recursive:true});
  writeFileSync("contracts/tools.schema.json",JSON.stringify({tools:PUBLIC_TOOLS},null,2)+"\n");
  writeFileSync("contracts/action-fixtures.json",JSON.stringify(payloadFixtures,null,2)+"\n");
  if(executed===0)throw new Error("empty Worker test suite");
  console.log(`Worker test files passed: ${executed}`);
} finally {
  rmSync(output, { recursive: true, force: true });
}

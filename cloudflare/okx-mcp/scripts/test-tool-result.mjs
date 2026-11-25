import { execFileSync } from "node:child_process";
import assert from "node:assert/strict";
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
  // These historical tests require a Cloudflare financial owner that no longer exists.
  // They are not current acceptance passes; invariant replacements are tracked in
  // FINANCIAL_WITNESS_RETIREMENT.md and the Native finance owner suite.
  console.log("RETIRED financial witnesses (not PASS): risk_book.test, legacy_finance_cutover.test, acceptance_composed.test; old financial cases of risk_authority.test replaced by retirement denial checks");
  let executed=0;
  for (const file of ["auth.test.js", "shared.test.js", "execution_tools.test.js", "account_governance.test.js", "account_governance_store.test.js", "risk_authority.test.js", "runtime_session.test.js", "runtime_backpressure.test.js", "mcp.test.js"]) {
    execFileSync(process.execPath, [join(output, file)], {
      stdio: "inherit",
    });
    executed++;
  }
  // The very same immutable inputs, canonical JSON and literal SHA256
  // are consumed by Rust protocol tests through include_str!.
  const fixture=JSON.parse(readFileSync(resolve(
    "../../contracts/trade-semantic-golden.json"),"utf8"));
  const risk=await import(pathToFileURL(join(output,"decimal.js")));
  const semantic=await import(pathToFileURL(join(output,"execution_tools.js")));
  // T01: fail CI if public MCP and Rust operator lists drift.
  const rustRegistry=readFileSync(resolve("../../crates/okx-strategy-core/src/operators.rs"),"utf8");
  const registryBlock=rustRegistry.match(/pub const SUPPORTED_OPERATORS:[\s\S]*?=\s*&\[([\s\S]*?)\];/);
  assert.ok(registryBlock,"Rust public operator registry exists");
  const rustNames=[...registryBlock[1].matchAll(/Operator::([A-Za-z]+)/g)]
    .map(([,name])=>name.replace(/[A-Z]/g,letter=>"_"+letter.toLowerCase()).slice(1));
  assert.deepEqual([...semantic.MARKET_OPERATORS],rustNames,
    "T01 public MCP operator IDs match the Rust registry order and names");
  console.log("T01 Rust/MCP operator catalog parity PASS",rustNames.length);
  // The composition graph has its own finite Rust enum. Every new primitive
  // must reach the strict public wire schema in the same slice.
  const rustPlan=readFileSync(resolve("../../crates/okx-research-protocol/src/strategy_contract.rs"),"utf8");
  const variants=rustPlan.match(/pub enum SeriesOp\s*\{([\s\S]*?)\}/);
  assert.ok(variants,"T15 Rust composition enum exists");
  const rustComposition=[...variants[1].matchAll(/^\s+([A-Z][A-Za-z]+),/gm)]
    .map(([,name])=>name.replace(/[A-Z]/g,letter=>"_"+letter.toLowerCase()).slice(1));
  const publicSchema=readFileSync(resolve("src/execution_tools.ts"),"utf8");
  const publicComposition=publicSchema.match(/const compositionStep=.*?op:choice\(([^)]*)\)/);
  assert.ok(publicComposition,"T15 public composition schema exists");
  const publicOps=[...publicComposition[1].matchAll(/'([a-z_]+)'/g)].map(([,id])=>id);
  assert.deepEqual(publicOps,rustComposition,"T15 every typed Rust series primitive has an MCP schema entry");
  console.log("T15 Rust/MCP series composition parity PASS",publicOps.length);
  for(const [input,expected] of fixture.numeric.positive){
    const result=risk.normalizeDecimal(input);
    assert.equal(result.canonical,expected,"S2 positive canonical "+input);
    assert.equal(risk.amount(risk.units(input)),expected,"S2 Worker exact roundtrip");
  }
  for(const [input,expected] of fixture.numeric.signed){
    assert.equal(risk.normalizeDecimal(input,true,false).canonical,expected,
      "S2 signed canonical "+input);
  }
  for(const [input,expected] of fixture.numeric.rejected){
    assert.equal(risk.normalizeDecimal(input).error_code,expected,
      "S2 strict negative "+input);
  }
  const [lhs,rhs,sum]=fixture.numeric.sum;
  assert.equal(risk.amount(risk.units(lhs)+risk.units(rhs)),sum,
    "S2 exact Worker tenth-decimal addition");
  for(const variant of fixture.trade.variants){
    assert.equal(semantic.canonicalTradeSemantic(fixture.trade.account_key,variant),
      fixture.trade.canonical_utf8_json,"S2 shared semantic canonical JSON");
    assert.equal(await semantic.tradeSemanticSha256(fixture.trade.account_key,variant),
      fixture.trade.sha256,"S2 shared literal semantic SHA256");
  }
  const tradeFromGolden=structuredClone(fixture.trade.variants[0]);
  tradeFromGolden.account="fixture-a";
  delete tradeFromGolden.order.reference; // absent optional, not explicit null in MCP
  assert.equal(semantic.validTool("trade",tradeFromGolden),true,
    "S2 public Worker trade schema accepts canonical decimal");
  tradeFromGolden.sizing.max_loss_usd="0.0000000001";
  assert.equal(semantic.validTool("trade",tradeFromGolden),true,
    "S2 public Worker trade schema accepts tenth fractional digit");
  tradeFromGolden.sizing.max_loss_usd="99999999999999999999999999999";
  assert.equal(semantic.validTool("trade",tradeFromGolden),false,
    "S2 public Worker trade schema rejects 29-digit input");
  tradeFromGolden.sizing.max_loss_usd="0";
  assert.equal(semantic.validTool("trade",tradeFromGolden),false,
    "S2 public Worker trade schema rejects zero risk");
  const changed=structuredClone(fixture.trade.variants[0]);
  changed.sizing.max_loss_usd="5.01";
  assert.notEqual(await semantic.tradeSemanticSha256(
      fixture.trade.account_key,changed),fixture.trade.sha256,
    "S2 changed amount requires a different semantic hash");
  const otherAccountKey="okx:real:"+"b".repeat(64);
  assert.notEqual(await semantic.tradeSemanticSha256(
      otherAccountKey,fixture.trade.variants[0]),fixture.trade.sha256,
    "S2 semantic digest is bound to the verified account identity");
  const changedExpiry=structuredClone(fixture.trade.variants[0]);
  changedExpiry.expires_in_ms=changedExpiry.expires_in_ms+1000;
  assert.notEqual(await semantic.tradeSemanticSha256(
      fixture.trade.account_key,changedExpiry),fixture.trade.sha256,
    "S2 distinct requested deadline must not replay the previous execution");
  const changedStrategy=structuredClone(fixture.trade.variants[0]);
  changedStrategy.strategy_instance="different";
  assert.notEqual(await semantic.tradeSemanticSha256(
      fixture.trade.account_key,changedStrategy),fixture.trade.sha256,
    "S2 immutable strategy identity must not collide on an idempotency key");
  console.log("S2 shared Rust/Worker golden numeric and semantic fixture PASS");
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

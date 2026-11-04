import test from "node:test";
import assert from "node:assert/strict";
import { assertBudgetEconomicsUnchanged } from "./budget-evidence.mjs";

const budget = () => ({
  source_as_of_ms: 1793658717543,
  campaign_limit_usd: "10", consumed_loss_usd: "0.006",
  reserved_loss_usd: "0", remaining_loss_usd: "9.994",
  occupied_modelled_loss_usdt: "0",
  campaign_origin: {campaign_id:"sha256:campaign", started_at_ms:1793658658348},
  entry_budget: {limit_usd:"10", consumed_usd:"6.003",
    reserved_usd:"0", remaining_usd:"3.997", opening_source_digest:"sha256:opening"},
});
test("fresh authenticated read changes source timestamp but not money", () => {
  const after=budget();after.source_as_of_ms+=5992;
  assert.doesNotThrow(()=>assertBudgetEconomicsUnchanged(budget(),after,"T09.8"));
});
test("negative: entry spend/reserve, loss, campaign identity may never differ", () => {
  for(const mutation of [
    b=>{b.entry_budget.consumed_usd="6.004"},
    b=>{b.entry_budget.reserved_usd="0.001"},
    b=>{b.reserved_loss_usd="0.001"},
    b=>{b.campaign_origin.campaign_id="sha256:another"},
  ]){
    const after=budget();mutation(after);after.source_as_of_ms+=1;
    assert.throws(()=>assertBudgetEconomicsUnchanged(budget(),after,"T09.8"),/must not mutate/);
  }
});
test("negative: missing or regressing source timestamp remains fail-closed", () => {
  for (const time of [null,0,1793658717542,"1793658717544"]) {
    const after=budget();after.source_as_of_ms=time;
    assert.throws(()=>assertBudgetEconomicsUnchanged(budget(),after,"T09.8"),/cannot disappear or regress/);
  }
});

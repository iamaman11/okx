// Read-only consistency assertion, not a money calculator or authority.
// A new authenticated account observation may advance the RiskBook source
// timestamp without changing any campaign/risk fields.
import assert from "node:assert/strict";

export function assertBudgetEconomicsUnchanged(before, after, reason) {
  assert.ok(before && after && typeof before === "object" &&
    typeof after === "object", reason + ": budgets required");
  const { source_as_of_ms: priorAsOf, ...priorEconomics } = before;
  const { source_as_of_ms: nextAsOf, ...nextEconomics } = after;
  assert.ok(Number.isSafeInteger(priorAsOf) && priorAsOf > 0 &&
    Number.isSafeInteger(nextAsOf) && nextAsOf >= priorAsOf,
  reason + ": authenticated source time cannot disappear or regress");
  assert.deepEqual(nextEconomics, priorEconomics, reason +
    ": rejection must not mutate any other risk/campaign/entry money field");
}

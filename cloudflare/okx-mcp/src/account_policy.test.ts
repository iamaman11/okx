import {
  accountPolicyRequest,
  defaultAccountPolicy,
  parseAccountPolicyUpdate,
  revisedAccountPolicy,
  validAccountPolicy,
  validOperatorRiskBudget,
} from "./account_policy.js";

function assert(condition: boolean, reason: string): void {
  if (!condition) throw Error(reason);
}
const a = "a".repeat(64);
const b = "b".repeat(64);
const initial = defaultAccountPolicy();
assert(validAccountPolicy(initial), "default is well typed, off, and capped");
assert(initial.revision === 0 && !initial.trading_requested && initial.max_total_modelled_loss_usdt === "10",
  "one global budget and fail-closed default");
for (const invalid of [
  {expected_revision: 0, trading_requested: true, allowed_account_uids: []},
  {expected_revision: 0, trading_requested: true, allowed_account_uids: ["A".repeat(64)]},
  {expected_revision: 0, trading_requested: true, allowed_account_uids: [a, a]},
  {expected_revision: 0, trading_requested: true, allowed_account_uids: [a], extra: true},
  {expected_revision: -1, trading_requested: false, allowed_account_uids: []},
  {expected_revision: 0, trading_requested: true, allowed_account_uids: [a], max_total_modelled_loss_usdt: "100"},
  {expected_revision: 0, trading_requested: false, allowed_account_uids: [], max_total_modelled_loss_usdt: "11"},
  {expected_revision: 0, trading_requested: false, allowed_account_uids: [], max_total_modelled_loss_usdt: "0"},
  {expected_revision: 0, trading_requested: false, allowed_account_uids: [], max_total_modelled_loss_usdt: "0.0000000001"},
  {expected_revision: 0, trading_requested: false, allowed_account_uids: [], max_total_modelled_loss_usdt: "1e0"},
]) assert(parseAccountPolicyUpdate(invalid) === null, "reject invalid/unbounded update");

for (const value of ["0.000000001","0.5","1","5.5","10","10.000000000"]) {
  assert(validOperatorRiskBudget(value), "valid selectable budget: " + value);
}
for (const value of ["","0","0.000000000","0.0000000001","-1","11","10.000000001","NaN","1e0",1,"01"]) {
  assert(!validOperatorRiskBudget(value), "reject unsafe budget: " + value);
}

const update = parseAccountPolicyUpdate({expected_revision: 0, trading_requested: true, allowed_account_uids: [b, a]});
assert(update !== null, "two-account update valid");
const on = revisedAccountPolicy(initial, update!);
assert(on !== null && on.revision === 1 && on.allowed_account_uids.join(",") === [a,b].join(","),
  "sorted accounts and monotonic revision");
assert(revisedAccountPolicy(on!, update!) === null, "replayed rev cannot reactivate trade");
const narrower = parseAccountPolicyUpdate({
  expected_revision: 1, trading_requested: true, allowed_account_uids: [a,b],
  max_total_modelled_loss_usdt: "3.25",
});
assert(narrower !== null, "revised global budget is operator data");
const adjusted = revisedAccountPolicy(on!, narrower!);
assert(adjusted?.revision === 2 && adjusted.max_total_modelled_loss_usdt === "3.25",
  "CAS can lower shared budget");
assert(revisedAccountPolicy(adjusted!, narrower!) === null, "old CAS cannot change budget");
const retained = revisedAccountPolicy(adjusted!, {
  expected_revision: 2, trading_requested: false, allowed_account_uids: [],
});
assert(retained?.max_total_modelled_loss_usdt === "3.25", "legacy update retains last budget");
assert(!validAccountPolicy({...on, max_total_modelled_loss_usdt: "11"}), "cannot enlarge cap");
assert(!validAccountPolicy({...on, revision: 0.5}), "revision must be integer");

class InMemoryStorage {
  private value: unknown;
  async get(_key: string): Promise<unknown> { return this.value; }
  async put(_key: string, value: unknown): Promise<void> { this.value = value; }
  async transaction<T>(fn: (tx: InMemoryStorage) => Promise<T>): Promise<T> { return fn(this); }
}
const store = new InMemoryStorage();
const cold = await accountPolicyRequest(store, "GET");
assert(cold.status === 200 && cold.body.effective_trading === false, "cold restart off");
const saved = await accountPolicyRequest(store, "POST", update);
assert(saved.status === 200 && saved.body.trading_requested === true
  && saved.body.effective_trading === false, "configuration never claims actual exchange rights");
assert((await accountPolicyRequest(store, "POST", update)).status === 409, "CAS prevents replay");
const revoked = await accountPolicyRequest(store, "POST", {
  expected_revision: 1, trading_requested: false, allowed_account_uids: [],
});
assert(revoked.status === 200 && revoked.body.revision === 2
  && revoked.body.trading_requested === false, "native revocation request persists");
assert((await accountPolicyRequest(store, "GET")).body.revision === 2, "durable owner readback");
const less = await accountPolicyRequest(store, "POST", {
  expected_revision: 2, trading_requested: false, allowed_account_uids: [],
  max_total_modelled_loss_usdt: "2.75",
});
assert(less.status === 200 && less.body.revision === 3
  && less.body.max_total_modelled_loss_usdt === "2.75"
  && less.body.effective_trading === false, "bounded CAS budget write not a trade permit");
assert((await accountPolicyRequest(store, "POST", {
  expected_revision: 3, trading_requested: false, allowed_account_uids: [],
  max_total_modelled_loss_usdt: "10.001",
})).status === 400, "never increase over operator authorization");
assert((await accountPolicyRequest(store, "GET")).body.max_total_modelled_loss_usdt === "2.75",
  "failed budget update leaves durable source unchanged");
await store.put("account_policy_v1", {fake:true});
assert((await accountPolicyRequest(store, "GET")).status === 503, "corrupt authority never becomes OFF silently");

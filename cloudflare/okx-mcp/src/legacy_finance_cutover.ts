// Historical financial authority retained only for retired test evidence.
// Not imported by the production Worker graph. See FINANCIAL_WITNESS_RETIREMENT.md.
import { GOVERNANCE_STORAGE_KEY, validGovernance } from "./account_governance.js";
import { sha256Hex } from "./shared.js";

// One irreversible retirement record in the ORIGINAL DO. No new budget,
// coordinator, renewal or grant endpoint. Pre-marker Worker rollbacks are unsafe.
export const LEGACY_FINANCE_DISABLED_KEY = "legacy_finance_disabled_v1";
export const LEGACY_FINANCE_DISABLED = "LEGACY_FINANCE_SERVER_DISABLED";
export interface LegacyFinanceDisableRequest {
  schema: "okx.legacy-finance-disable/v1";
  installation_id: string;
  operation_id: string;
  source_tree: string;
}
export interface LegacyFinanceDisabledReceipt extends LegacyFinanceDisableRequest {
  state: "DISABLED";
  disabled_at_ms: number;
  inventory_sha256: string;
  inventory_records: number;
}
type Reply = { status: number; body: Record<string, unknown> };
const failure = (reason: string, status = 409): Reply => ({status, body:{status:"FAILED", reason}});
export async function legacyFinanceDisabled(tx: any): Promise<boolean> {
  // Corrupt/unknown marker is also fail-closed; never interpret it as enabled.
  return await tx.get(LEGACY_FINANCE_DISABLED_KEY) !== undefined;
}
function canonical(value: any): string {
  if (Array.isArray(value)) return "[" + value.map(canonical).join(",") + "]";
  if (value !== null && typeof value === "object") return "{" + Object.keys(value).sort()
    .map(key => JSON.stringify(key) + ":" + canonical(value[key])).join(",") + "}";
  return JSON.stringify(value);
}
function validRequest(raw: any): raw is LegacyFinanceDisableRequest {
  return raw !== null && typeof raw === "object" && !Array.isArray(raw)
    && Object.keys(raw).sort().join(",") === "installation_id,operation_id,schema,source_tree"
    && raw.schema === "okx.legacy-finance-disable/v1"
    && typeof raw.installation_id === "string" && raw.installation_id.length > 0
    && raw.installation_id.length <= 128
    && typeof raw.operation_id === "string" && /^[A-Za-z0-9_-]{8,128}$/.test(raw.operation_id)
    && typeof raw.source_tree === "string" && /^[0-9a-f]{40}$/.test(raw.source_tree);
}
function validReceipt(raw: any): raw is LegacyFinanceDisabledReceipt {
  if (raw === null || typeof raw !== "object") return false;
  const {state, disabled_at_ms, inventory_sha256, inventory_records, ...request} = raw;
  return validRequest(request) && state === "DISABLED"
    && Number.isSafeInteger(disabled_at_ms) && disabled_at_ms > 0
    && typeof inventory_sha256 === "string" && /^[0-9a-f]{64}$/.test(inventory_sha256)
    && Number.isSafeInteger(inventory_records) && inventory_records >= 2;
}
// All original durable keys, including unknown/legacy formats and policy
// idempotency receipts, are preserved. Only transport generation and this
// retirement record are excluded. Unknown source formats must be rejected by
// importers rather than silently omitted or treated as zero obligations.
// Consumers must validate those formats; this endpoint never asserts EMPTY.
async function inventory(tx: any): Promise<Record<string, unknown>> {
  const result: Record<string, unknown> = {};
  let startAfter: string | undefined;
  for (;;) {
    const page: Map<string, unknown> = await tx.list({prefix:"", limit:1000,
      ...(startAfter === undefined ? {} : {startAfter})});
    for (const [key, value] of page) {
      if ((startAfter !== undefined && key <= startAfter)
          || key in result) throw Error("LEGACY_FINANCE_INVENTORY_ORDER_INVALID");
      startAfter = key;
      if(key === "generation" || key === LEGACY_FINANCE_DISABLED_KEY)continue;
      result[key] = value;
      if (Object.keys(result).length > 10000) throw Error("LEGACY_FINANCE_INVENTORY_TOO_LARGE");
    }
    if (page.size < 1000) break;
  }
  const governance = await tx.get(GOVERNANCE_STORAGE_KEY);
  if (!validGovernance(governance)) throw Error("LEGACY_FINANCE_INSTALLATION_UNPROVEN");
  result[GOVERNANCE_STORAGE_KEY] = governance;
  const release = await tx.get("installation_release_v1");
  if (release !== undefined) result.installation_release_v1 = release;
  const meta = result.risk_meta_v2 as any;
  if (!meta || meta.installation_id !== governance.installation_id)
    throw Error("LEGACY_FINANCE_SCOPE_UNPROVEN");
  return result;
}
export async function disableLegacyFinance(storage: any, raw: unknown, now: number): Promise<Reply> {
  if (!validRequest(raw) || !Number.isSafeInteger(now) || now <= 0)
    return failure("INVALID_LEGACY_FINANCE_DISABLE", 400);
  return storage.transaction(async (tx: any): Promise<Reply> => {
    const previous = await tx.get(LEGACY_FINANCE_DISABLED_KEY);
    if (previous !== undefined) {
      if (!validReceipt(previous)) return failure("LEGACY_FINANCE_DISABLE_RECEIPT_INVALID");
      const {state:_, disabled_at_ms:__, inventory_sha256:___, inventory_records:____, ...request} = previous;
      return canonical(request) === canonical(raw)
        ? {status:200, body:{receipt:previous}} : failure("LEGACY_FINANCE_DISABLE_CONFLICT");
    }
    const sources = await inventory(tx);
    const governance = sources[GOVERNANCE_STORAGE_KEY] as any;
    if (governance.installation_id !== raw.installation_id) return failure("INSTALLATION_ID_MISMATCH");
    const receipt: LegacyFinanceDisabledReceipt = {...raw, state:"DISABLED", disabled_at_ms:now,
      inventory_sha256:await sha256Hex(canonical(sources)), inventory_records:Object.keys(sources).length};
    await tx.put(LEGACY_FINANCE_DISABLED_KEY, receipt);
    return {status:200, body:{receipt}};
  });
}
export async function inspectLegacyFinanceCutover(storage: any): Promise<Reply> {
  return storage.transaction(async (tx: any): Promise<Reply> => {
    const receipt = await tx.get(LEGACY_FINANCE_DISABLED_KEY);
    if (receipt === undefined) return failure("LEGACY_FINANCE_NOT_DISABLED");
    if (!validReceipt(receipt)) return failure("LEGACY_FINANCE_DISABLE_RECEIPT_INVALID");
    const sources = await inventory(tx);
    if ((sources[GOVERNANCE_STORAGE_KEY] as any).installation_id !== receipt.installation_id
        || Object.keys(sources).length !== receipt.inventory_records
        || await sha256Hex(canonical(sources)) !== receipt.inventory_sha256)
      return failure("LEGACY_FINANCE_INVENTORY_CHANGED");
    return {status:200, body:{schema:"okx.legacy-finance-readback/v1", state:"DISABLED",
      inventory_complete:true, receipt, sources}};
  });
}

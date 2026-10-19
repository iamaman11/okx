// One versioned operator policy for every authenticated account, kept in the
// EXISTING Cloudflare RuntimeSession durable object. This is configuration,
// not an exchange POST permit. Rust must validate the actual account UID,
// venue permissions, NAV, STOP, and one global risk budget before any live POST.
export const ACCOUNT_POLICY_SCHEMA = "okx.account-policy/v1" as const;
// Authorization ceiling for this operator; budget itself is policy data.
export const AUTHORIZED_RISK_CEILING_USDT = "10" as const;
const KEY = "account_policy_v1";

export interface AccountPolicy {
  schema: typeof ACCOUNT_POLICY_SCHEMA;
  revision: number;
  trading_requested: boolean;
  allowed_account_uids: string[];
  max_total_modelled_loss_usdt: string;
}

export type AccountPolicyUpdate = {
  expected_revision: number;
  trading_requested: boolean;
  allowed_account_uids: string[];
  max_total_modelled_loss_usdt?: string;
};

// Exact monetary parsing; CAS may lower the selected budget but cannot
// raise the outer ceiling of this operator's current authorization.
export function validOperatorRiskBudget(value: unknown): value is string {
  if (typeof value !== "string" || !/^(?:0|[1-9][0-9]?)(?:\.[0-9]{1,9})?$/.test(value)) return false;
  const [whole, fraction = ""] = value.split(".");
  const units = BigInt(whole) * 1_000_000_000n
    + BigInt(fraction.padEnd(9, "0") || "0");
  const ceiling = BigInt(AUTHORIZED_RISK_CEILING_USDT) * 1_000_000_000n;
  return units > 0n && units <= ceiling;
}

export function defaultAccountPolicy(): AccountPolicy {
  return {
    schema: ACCOUNT_POLICY_SCHEMA,
    revision: 0,
    trading_requested: false,
    allowed_account_uids: [],
    max_total_modelled_loss_usdt: AUTHORIZED_RISK_CEILING_USDT,
  };
}

function isLowerSha256(value: unknown): value is string {
  return typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
}

export function validAccountPolicy(value: unknown): value is AccountPolicy {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const p = value as Record<string, unknown>;
  if (Object.keys(p).sort().join(",") !==
      "allowed_account_uids,max_total_modelled_loss_usdt,revision,schema,trading_requested") return false;
  if (p.schema !== ACCOUNT_POLICY_SCHEMA
      || !Number.isSafeInteger(p.revision) || (p.revision as number) < 0
      || typeof p.trading_requested !== "boolean"
      || !validOperatorRiskBudget(p.max_total_modelled_loss_usdt)
      || !Array.isArray(p.allowed_account_uids)
      || p.allowed_account_uids.length > 16
      || !p.allowed_account_uids.every(isLowerSha256)
      || new Set(p.allowed_account_uids).size !== p.allowed_account_uids.length
      || (p.trading_requested && p.allowed_account_uids.length === 0)) return false;
  return true;
}

export function parseAccountPolicyUpdate(value: unknown): AccountPolicyUpdate | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return null;
  const request = value as Record<string, unknown>;
  const fields = Object.keys(request).sort().join(",");
  if (fields !== "allowed_account_uids,expected_revision,trading_requested"
      && fields !== "allowed_account_uids,expected_revision,max_total_modelled_loss_usdt,trading_requested")
    return null;
  if (!Number.isSafeInteger(request.expected_revision)
      || (request.expected_revision as number) < 0
      || typeof request.trading_requested !== "boolean"
      || !Array.isArray(request.allowed_account_uids)
      || request.allowed_account_uids.length > 16
      || !request.allowed_account_uids.every(isLowerSha256)
      || new Set(request.allowed_account_uids).size !== request.allowed_account_uids.length
      || (request.trading_requested && request.allowed_account_uids.length === 0)
      || (request.max_total_modelled_loss_usdt !== undefined
          && !validOperatorRiskBudget(request.max_total_modelled_loss_usdt))) return null;
  return request as AccountPolicyUpdate;
}

export function revisedAccountPolicy(
  previous: AccountPolicy,
  update: AccountPolicyUpdate,
): AccountPolicy | null {
  if (!validAccountPolicy(previous) || update.expected_revision !== previous.revision
      || !Number.isSafeInteger(previous.revision + 1)) return null;
  return {
    schema: ACCOUNT_POLICY_SCHEMA,
    revision: previous.revision + 1,
    trading_requested: update.trading_requested,
    allowed_account_uids: [...update.allowed_account_uids].sort(),
    max_total_modelled_loss_usdt: update.max_total_modelled_loss_usdt
      ?? previous.max_total_modelled_loss_usdt,
  };
}

export function publicAccountPolicy(policy: AccountPolicy): Record<string, unknown> {
  return {
    ...policy,
    status: "CONFIGURED",
    // There is no lie about authority: a configured central wish is NOT a
    // native admission receipt. The existing Rust executor stays fail-closed.
    effective_trading: false,
    effective_reason: "REQUIRES_NATIVE_UID_CREDENTIAL_FINANCIAL_AND_RISK_ATTESTATION",
  };
}

// The only persistent update owner is the existing Durable Object, so
// concurrent OAuth clients are fenced by one atomic revision transaction.
export async function accountPolicyRequest(
  storage: any,
  method: string,
  input?: unknown,
): Promise<{ body: Record<string, unknown>; status: number }> {
  if (method === "GET") {
    if (await storage.get("account_governance_v2") !== undefined)
      return {status:409,body:{status:"FAILED",reason:"POLICY_V1_RETIRED_USE_ACCOUNT_GOVERNANCE"}};
    const stored = await storage.get(KEY) as unknown;
    if (stored !== undefined && !validAccountPolicy(stored)) {
      return { status: 503, body: { status: "FAILED", reason: "POLICY_CORRUPT" } };
    }
    return { status: 200, body: publicAccountPolicy(stored ?? defaultAccountPolicy()) };
  }
  if (method !== "POST") {
    return { status: 405, body: { status: "FAILED", reason: "METHOD_NOT_ALLOWED" } };
  }
  // Once v2 is initialized, old v1 configuration cannot become an alternate
  // positive authority. Migration is monotonic and fails closed.
  const update = parseAccountPolicyUpdate(input);
  if (!update) return { status: 400, body: { status: "FAILED", reason: "INVALID_POLICY_UPDATE" } };
  return storage.transaction(async (tx: any) => {
    // The v1 retirement fence MUST be in the same DO transaction as the write.
    if (await tx.get("account_governance_v2") !== undefined)
      return {status:409,body:{status:"FAILED",reason:"POLICY_V1_RETIRED_USE_ACCOUNT_GOVERNANCE"}};
    const stored = await tx.get(KEY) as unknown;
    if (stored !== undefined && !validAccountPolicy(stored)) {
      return { status: 503, body: { status: "FAILED", reason: "POLICY_CORRUPT" } };
    }
    const previous = (stored ?? defaultAccountPolicy()) as AccountPolicy;
    const next = revisedAccountPolicy(previous, update);
    if (!next) return {
      status: 409,
      body: { status: "FAILED", reason: "POLICY_REVISION_CONFLICT", revision: previous.revision },
    };
    await tx.put(KEY, next);
    return { status: 200, body: publicAccountPolicy(next) };
  });
}

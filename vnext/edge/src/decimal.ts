/** Exact decimal normalization for typed transport and configuration. No financial storage. */
const U = 10n ** 28n;
const MAX_DECIMAL_DIGITS = 28;
const AMOUNT = /^(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$/;
export type DecimalResult =
  | { canonical: string; error_code: null }
  | { canonical: null; error_code: "NUMERIC_RANGE" | "INVALID_DECIMAL_SYNTAX" | "DECIMAL_MUST_BE_POSITIVE" };
export function normalizeDecimal(
  value: unknown,
  signed = false,
  requirePositive = true,
): DecimalResult {
  if (typeof value !== "string") return {canonical:null,error_code:"INVALID_DECIMAL_SYNTAX"};
  const negative=value.startsWith("-");
  if (negative&&!signed) return {canonical:null,error_code:"INVALID_DECIMAL_SYNTAX"};
  const body=negative?value.slice(1):value;
  if (!AMOUNT.test(body)) return {canonical:null,error_code:"INVALID_DECIMAL_SYNTAX"};
  const [whole, fraction=""]=body.split(".");
  if (whole.length+fraction.length>MAX_DECIMAL_DIGITS)
    return {canonical:null,error_code:"NUMERIC_RANGE"};
  const fractional=fraction.replace(/0+$/,"");
  if (!/[1-9]/.test(whole+fraction))
    return requirePositive
      ?{canonical:null,error_code:"DECIMAL_MUST_BE_POSITIVE"}
      :{canonical:"0",error_code:null};
  const canonical=(negative?"-":"")+whole+(fractional?"."+fractional:"");
  return {canonical,error_code:null};
}
export function units(value: unknown): bigint | null {
  const normalized=normalizeDecimal(value,false,false);
  if (normalized.canonical===null) return null;
  const [whole, fraction = ""] = normalized.canonical.split(".");
  return BigInt(whole) * U + BigInt(fraction.padEnd(28, "0") || "0");
}
export function amount(value: bigint): string {
  if (value < 0n) throw Error("negative risk");
  const whole = (value / U).toString();
  const fraction = (value % U).toString().padStart(28, "0").replace(/0+$/, "");
  if (whole.length + fraction.length > MAX_DECIMAL_DIGITS)
    throw Error("NUMERIC_RANGE");
  return fraction ? whole + "." + fraction : whole;
}

import {
  TOOL_TEXT_FALLBACK_MAX_BYTES,
  toolResult,
  type Json,
} from "./shared.js";

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(message);
}

const value: Json = {
  schema: "okx.agent.response/v1",
  status: "completed",
  quality: "FRESH",
  result_schema: "okx.account-summary/v1",
  result: {
    positions: 0,
    pending_orders: 0,
    marker: "payload-that-must-not-be-duplicated",
  },
};

const wrapped = toolResult(value) as {
  content: Array<{ type: string; text: string }>;
  structuredContent: Json;
};

assert(
  wrapped.structuredContent === value,
  "toolResult must preserve the exact structuredContent object",
);
assert(wrapped.content.length === 1, "toolResult must emit one compact text fallback");
assert(wrapped.content[0]?.type === "text", "toolResult fallback must remain MCP text content");

const text = wrapped.content[0]?.text ?? "";
const textBytes = new TextEncoder().encode(text).byteLength;
assert(
  textBytes <= TOOL_TEXT_FALLBACK_MAX_BYTES,
  `toolResult text fallback exceeds budget: ${textBytes} bytes`,
);
assert(
  !text.includes(JSON.stringify(value)),
  "toolResult must not duplicate the full structured payload in content[].text",
);
assert(
  !text.includes("payload-that-must-not-be-duplicated"),
  "toolResult text fallback must not leak nested structured payload content",
);

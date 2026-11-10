// Test-only transport fault. The deployed RuntimeSession still owns parsing,
// authorization and every SQLite transaction. Only the selected committed
// response is withheld; the parent subsequently kills the real OS process.
// This entry is never imported by the deployed Worker.
import { RuntimeSession as ProductionRuntimeSession } from "../runtime_session.js";
export { default } from "./runtime_entry.js";

const withheldCommands = new Set([
  "reserve_lost_reply_a_0001",
  "t10_12_settlement_reply_fenced_0001",
]);

export class RuntimeSession extends ProductionRuntimeSession {
  async webSocketMessage(ws: any, message: string | ArrayBuffer): Promise<void> {
    let commandId: unknown;
    if (typeof message === "string") {
      try { commandId = JSON.parse(message)?.command?.command_id; }
      catch { /* The production parser handles invalid input. */ }
    }
    if (typeof commandId !== "string" || !withheldCommands.has(commandId)) {
      return super.webSocketMessage(ws, message);
    }
    const selectedCommandId = commandId;
    const transport = new Proxy(ws, {
      get(target, property) {
        if (property === "send") return (data: string) => {
          const frame = JSON.parse(data);
          if (frame.type === "risk_control_result" &&
              frame.result?.command_id === selectedCommandId) {
            // Reached only AFTER the actual handler and its durable transaction
            // returned, immediately BEFORE the production ws.send would run.
            // Emit only bounded correlation metadata, never raw WSS payloads
            // or signed source/account credentials.
            console.log("T10_12_WORKER_REPLY_FENCED_V1:" + JSON.stringify({
              schema:"okx.t10-12-fenced-response/v1",
              command_id:frame.result.command_id,
              session_id:frame.session_id,
              connection_generation:frame.connection_generation,
              status:frame.result.status,
              reservation_key:frame.result.reservation_key,
              risk_book_sequence:frame.result.risk_book_sequence,
            }));
            return;
          }
          target.send(data);
        };
        const value = Reflect.get(target, property, target);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    await super.webSocketMessage(transport, message);
  }
}

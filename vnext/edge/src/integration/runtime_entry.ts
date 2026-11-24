// Test-only forwarding into the exact deployed Durable Object class.
// It is not imported from src/index.ts and exposes no new production endpoint.
import { RuntimeSession } from "../runtime_session.js";
export { RuntimeSession };

type DurableBinding = {
  idFromName(name: string): unknown;
  get(id: unknown): { fetch(request: Request): Promise<Response> };
};
export default {
  fetch(request: Request, env: { RUNTIME: DurableBinding }): Promise<Response> {
    return env.RUNTIME.get(env.RUNTIME.idFromName("native-risk-integration")).fetch(request);
  },
};

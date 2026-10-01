import OAuthProvider from "@cloudflare/workers-oauth-provider";

import { defaultHandler } from "./auth";
import { mcpApi } from "./mcp";
import { RuntimeSession } from "./runtime_session";
import { PUBLIC_ORIGIN, type Env } from "./shared";

export { RuntimeSession };

export default new OAuthProvider<Env>({
  apiRoute: "/mcp",
  apiHandler: mcpApi,
  defaultHandler,
  authorizeEndpoint: "/authorize",
  tokenEndpoint: "/token",
  clientRegistrationEndpoint: "/register",
  scopesSupported: ["mcp:use"],
  resourceMetadata: {
    resource: `${PUBLIC_ORIGIN}/mcp`,
    authorization_servers: [PUBLIC_ORIGIN],
  },
  requiredScopes: ["mcp:use"],
});

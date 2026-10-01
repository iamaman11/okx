import {
  AUTH_APPROVAL_TTL_MS,
  AUTH_CSRF_TTL_MS,
  MCP_SERVER_VERSION,
  TOOL_CONTRACT_VERSION,
  type Env,
  runtimeStub,
  sameHex,
  sha256Hex,
} from "./shared";

async function hmacSha256Hex(secret: string, value: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(value));
  return [...new Uint8Array(signature)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

function authorizationRequestUrl(url: URL): URL {
  const clean = new URL(url.toString());
  clean.searchParams.delete("approval");
  clean.hash = "";
  return clean;
}

function canonicalAuthorizationBinding(url: URL): string {
  const canonical = authorizationRequestUrl(url);
  canonical.searchParams.sort();
  return `${canonical.origin}${canonical.pathname}?${canonical.searchParams.toString()}`;
}

function authorizationTokenMessage(kind: "csrf" | "approval", url: URL, expiresAtMs: number): string {
  return [
    `okx.oauth.${kind}/v1`,
    String(expiresAtMs),
    canonicalAuthorizationBinding(url),
  ].join("\n");
}

async function issueAuthorizationToken(
  env: Env,
  kind: "csrf" | "approval",
  url: URL,
  ttlMs: number,
): Promise<string> {
  const expiresAtMs = Date.now() + ttlMs;
  const signature = await hmacSha256Hex(
    env.MCP_OWNER_SECRET,
    authorizationTokenMessage(kind, url, expiresAtMs),
  );
  return `${expiresAtMs}.${signature}`;
}

async function verifyAuthorizationToken(
  env: Env,
  kind: "csrf" | "approval",
  url: URL,
  token: string,
  ttlMs: number,
): Promise<boolean> {
  const separator = token.indexOf(".");
  if (separator <= 0) return false;
  const expiresText = token.slice(0, separator);
  const suppliedSignature = token.slice(separator + 1);
  if (!/^\d{13}$/.test(expiresText) || !/^[0-9a-f]{64}$/.test(suppliedSignature)) return false;

  const expiresAtMs = Number(expiresText);
  const now = Date.now();
  if (!Number.isSafeInteger(expiresAtMs) || expiresAtMs < now || expiresAtMs - now > ttlMs) {
    return false;
  }

  const expectedSignature = await hmacSha256Hex(
    env.MCP_OWNER_SECRET,
    authorizationTokenMessage(kind, url, expiresAtMs),
  );
  return sameHex(suppliedSignature, expectedSignature);
}

async function issueAuthorizationCsrf(env: Env, url: URL): Promise<string> {
  return issueAuthorizationToken(env, "csrf", url, AUTH_CSRF_TTL_MS);
}

async function verifyAuthorizationCsrf(env: Env, url: URL, token: string): Promise<boolean> {
  return verifyAuthorizationToken(env, "csrf", url, token, AUTH_CSRF_TTL_MS);
}

async function issueAuthorizationApproval(env: Env, url: URL): Promise<string> {
  return issueAuthorizationToken(env, "approval", url, AUTH_APPROVAL_TTL_MS);
}

async function verifyAuthorizationApproval(env: Env, url: URL, token: string): Promise<boolean> {
  return verifyAuthorizationToken(env, "approval", url, token, AUTH_APPROVAL_TTL_MS);
}

async function completeOwnerAuthorization(env: Env, authRequest: any): Promise<Response> {
  const { redirectTo } = await env.OAUTH_PROVIDER.completeAuthorization({
    request: authRequest,
    userId: "okx-owner",
    metadata: {},
    scope: authRequest.scope,
    props: { user: "okx-owner" },
    revokeExistingGrants: false,
  });
  return Response.redirect(redirectTo, 302);
}

export const defaultHandler = {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return Response.json({
        status: "ok",
        service: "okx-cloudflare-mcp",
        server_version: MCP_SERVER_VERSION,
        tool_contract: TOOL_CONTRACT_VERSION,
      });
    }

    if (url.pathname === "/runtime") {
      const stub = await runtimeStub(env);
      return stub.fetch(request);
    }

    if (url.pathname !== "/authorize") {
      return new Response("not found", { status: 404 });
    }

    const oauthUrl = authorizationRequestUrl(url);
    let authRequest: any;
    try {
      authRequest = await env.OAUTH_PROVIDER.parseAuthRequest(new Request(oauthUrl.toString()));
    } catch {
      return new Response("invalid authorization request", { status: 400 });
    }

    if (request.method === "POST") {
      const wantsJson = request.headers.get("accept")?.includes("application/json") ?? false;
      const form = await request.formData();
      const csrf = String(form.get("csrf") ?? "");
      if (!(await verifyAuthorizationCsrf(env, oauthUrl, csrf))) {
        if (wantsJson) {
          return Response.json(
            { status: "FAILED", reason: "INVALID_CSRF" },
            { status: 400, headers: { "cache-control": "no-store" } },
          );
        }
        return new Response("invalid authorization csrf", {
          status: 400,
          headers: { "cache-control": "no-store" },
        });
      }

      const supplied = String(form.get("secret") ?? "");
      const [suppliedHash, expectedHash] = await Promise.all([
        sha256Hex(supplied),
        sha256Hex(env.MCP_OWNER_SECRET),
      ]);
      if (!sameHex(suppliedHash, expectedHash)) {
        if (wantsJson) {
          return Response.json(
            { status: "FAILED", reason: "INVALID_OWNER_SECRET" },
            { headers: { "cache-control": "no-store" } },
          );
        }
        return authorizationPage(oauthUrl, authRequest, true, env);
      }

      const approval = await issueAuthorizationApproval(env, oauthUrl);
      const continueUrl = new URL(oauthUrl.toString());
      continueUrl.searchParams.set("approval", approval);
      if (wantsJson) {
        return Response.json(
          { status: "PASS", continue_url: continueUrl.pathname + continueUrl.search },
          { headers: { "cache-control": "no-store" } },
        );
      }
      return Response.redirect(continueUrl.toString(), 303);
    }

    if (request.method === "GET") {
      const approval = url.searchParams.get("approval");
      if (approval !== null) {
        if (!(await verifyAuthorizationApproval(env, oauthUrl, approval))) {
          return new Response("invalid or expired authorization approval", {
            status: 400,
            headers: { "cache-control": "no-store" },
          });
        }
        return completeOwnerAuthorization(env, authRequest);
      }
      return authorizationPage(oauthUrl, authRequest, false, env);
    }
    return new Response("method not allowed", { status: 405 });
  },
};

async function authorizationPage(url: URL, authRequest: any, invalid: boolean, env: Env): Promise<Response> {
  const action = escapeHtml(url.pathname + url.search);
  const csrf = escapeHtml(await issueAuthorizationCsrf(env, url));
  const scopes = Array.isArray(authRequest.scope) ? authRequest.scope.join(" ") : "";
  const scriptNonce = crypto.randomUUID().replaceAll("-", "");
  const body = `<!doctype html>
<meta charset="utf-8">
<title>Authorize OKX MCP</title>
<h1>Authorize OKX Cloudflare MCP</h1>
<p>Client requests access to the Windows OKX read transport.</p>
<p>Requested scope: <code>${escapeHtml(scopes)}</code></p>
<p>The owner secret is sent only in an HTTPS POST body. OAuth completion then continues with a short-lived signed GET approval, matching the proven ChatGPT flow.</p>
${invalid ? '<p id="auth-status">Invalid owner secret.</p>' : '<p id="auth-status" aria-live="polite"></p>'}
<form id="owner-auth" method="post" action="${action}">
<input type="hidden" name="csrf" value="${csrf}">
<label>Owner secret <input type="password" name="secret" autocomplete="current-password" required></label>
<button id="authorize-button" type="submit">Authorize</button>
</form>
<script nonce="${scriptNonce}">
(() => {
  const form = document.getElementById("owner-auth");
  const button = document.getElementById("authorize-button");
  const status = document.getElementById("auth-status");
  if (!(form instanceof HTMLFormElement) || !(button instanceof HTMLButtonElement) || !(status instanceof HTMLElement)) return;

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    button.disabled = true;
    status.textContent = "Authorizing…";
    try {
      const response = await fetch(form.action, {
        method: "POST",
        body: new FormData(form),
        headers: { "accept": "application/json" },
        credentials: "same-origin",
        cache: "no-store",
      });
      const result = await response.json();
      if (response.ok && result?.status === "PASS" && typeof result.continue_url === "string") {
        const link = document.createElement("a");
        link.href = result.continue_url;
        link.textContent = "Authorize and continue";
        status.replaceChildren(link);
        link.click();
        return;
      }
      status.textContent = result?.reason === "INVALID_OWNER_SECRET"
        ? "Invalid owner secret."
        : "Authorization request failed. Try again.";
    } catch {
      status.textContent = "Embedded authorization transport failed; retrying with browser navigation…";
      HTMLFormElement.prototype.submit.call(form);
      return;
    } finally {
      button.disabled = false;
    }
  });
})();
</script>`;
  return new Response(body, {
    headers: {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
      "referrer-policy": "no-referrer",
      "content-security-policy": `default-src 'none'; script-src 'nonce-${scriptNonce}'; connect-src 'self'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'`,
    },
  });
}

function escapeHtml(value: string): string {
  return value.replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[char] ?? char);
}

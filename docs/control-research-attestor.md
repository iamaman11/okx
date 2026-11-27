# CONTROL → Research process attestation (#634)

The existing Edge Research WebSocket authenticates the Research **connection**.
It does not independently establish the process identity or the installed
binary. The optional CONTROL attestor adds a separate read-only observation of
the same live Research connection through the existing Durable Object.

## Trust and failure model

- CONTROL has its own `/attestor` WebSocket identity; neither the Engine nor
  Research runtime credential can authenticate this channel.
- Edge challenges CONTROL with a one-use random nonce, bounded deadline,
  CONTROL session/generation, and the **current** Research session/generation.
  CONTROL serializes a fresh `ResearchRuntimeStatus` readback through its
  existing single-owner supervisor loop; it does not start, stop, or reconcile
  Research for attestation.
- Edge verifies the typed status, running PID, active and installed image SHA,
  and a still-current Research WebSocket before reporting process proof `PASS`.
  A timeout, missing credential, rotation, changed generation, or mismatch
  remains `partial`; no persistent positive attestation is cached.
- The reported SHA represents CONTROL's independently observed selected binary
  **file**, not an in-memory hash of the executing process. CONTROL and Research
  run in the same Windows security context: a distinct credential protects
  against accidental bearer/owner confusion, **not** a malicious process that
  has already compromised that Windows account.
- The current v1 proof matches CONTROL's observed PID and image SHA against
  the authenticated Research WebSocket's claims; it does not independently
  match the per-launch Research incarnation. A holder of a stolen Research
  bearer who also knows the live PID and image SHA could impersonate that
  socket. Do not interpret `PASS` as protection against stolen credentials.

This channel has no `/dispatch`, finance, account, installation, Research
lifecycle, or GitHub-control operations. Its disconnect does not close Engine
or Research runtime sockets.

## Disabled-by-default rollout

1. Complete the required source, Windows, Worker, and negative protocol CI.
   Do not infer a production `PASS` from CI fixtures alone.
2. In the protected GitHub deploy environment set the optional
   `OKX_CONTROL_ATTEST_TOKEN_SHA256` secret to the independent fingerprint;
   the existing `cloudflare-mcp-deploy.yml` passes it as
   `CONTROL_ATTEST_TOKEN_SHA256` only into the release configuration. Missing
   secret generates the explicit invalid `disabled` binding and keeps production
   attestation disabled, even if an earlier out-of-band Wrangler secret used
   the same name (Cloudflare replaces that secret when deploying this variable).
   Deploy the accepted Worker version with this fingerprint. It must be
   exactly 64 lowercase hex characters corresponding to a **new** CONTROL-only token generated from
   32 cryptographically random bytes and encoded as 64 lowercase hex digits;
   never use the GitHub, Engine, or Research bearer. Store the actual token
   only in Windows Credential Manager using the explicit CONTROL
   `set-attestor-token` stdin command. Never record it in Git, logs, or issues.
3. Activate the independently accepted CONTROL binary through the normal
   verified update path. An existing installation without that credential
   remains operational and returns `partial` for this proof.
4. Verify `diagnostics.explain(view=research, reference=deployment)` against
   the live native Research process. Independently confirm CONTROL's process
   generation, PID, and selected file SHA; exercise offline, stale, restarted,
   mismatched, and revoked-token scenarios before closing #634.

For revocation, invalidate/remove the Edge fingerprint and separately remove
the Windows CONTROL-only credential; a previously admitted attestor socket
must lose authority after an Edge fingerprint change. Rotate by provisioning
a new independent bearer on both ends, allowing only the new fingerprint to
authenticate. There is no implicit credential reuse or fallback.

On CONTROL builds with attestor credential refresh, an **already enabled**
attestor checks its Windows credential on the existing 20-second heartbeat
and re-reads it before each reconnect. Rotation or revocation ends only that
attestor socket; CONTROL and its Engine/Research children are not restarted.
First-time enablement on a CONTROL process started without the credential
still requires normal verified CONTROL activation. No live secret rotation is
part of a read-only diagnostic check.

Do not publish workstation identifiers, process paths, tokens, or live
fingerprints in public issue comments or pull-request logs.

## Hermetic negative acceptance versus physical proof

The Cloudflare CI test roster runs
`scripts/test-control-attestor-do.mjs` inside the installed Miniflare/workerd
runtime. It bundles the production Worker bearer prefilter and
`RuntimeSession` Durable Object behind a **test-only** local entrypoint
(`src/integration/control_attestor_entry.ts`). Every request uses local
`dispatchFetch`; Research and CONTROL connect as independent WebSocket
clients with cryptographically generated, per-run fixture credentials. Real
SQLite Durable Object state is retained across local workerd restarts. No
production Worker route, Wrangler configuration, Windows service, deployed
secret, Engine lifecycle, or finance operation is touched.

| CI witness | Expected fail-closed result |
| --- | --- |
| Missing/cross-owner bearer, invalid first CONTROL hello | 401/1008; no authenticated CONTROL observation |
| Research connected but CONTROL absent or disconnected | `CONTROL_ATTEST_OFFLINE` |
| PID, installed SHA, or phase differs from Research WSS | `CONTROL_RESEARCH_PID_SHA_MISMATCH` |
| Replayed completed observation | 1008; no previous PASS reuse |
| Research WSS replacement during challenge | `RESEARCH_EDGE_GENERATION_CHANGED` |
| CONTROL WSS replacement during challenge | `CONTROL_ATTEST_SOCKET_STALE`; Research remains independently live |
| Three unanswered challenges plus a fourth | `CONTROL_ATTEST_BUSY`; bounded admission |
| Missing answer past the Edge 2.5-second deadline | `CONTROL_ATTEST_TIMEOUT`; subsequent fresh challenge can PASS |
| Persisted DO restarted with disabled/rotated test fingerprint | Disabled 503; old bearer 401; only fresh owner handshake/challenge can PASS |

These fixtures are **physical-like CI only**. Cold binding changes simulate
revocation/rotation across local workerd restart; they do **not** establish
that an already-admitted production socket loses authority during live
in-place secret rotation (that property has a separate in-memory DO test).
They also cannot establish Windows process PID/SHA provenance, physical
Research restarts, host Credential Manager behavior, or real Edge rollout.
The controlled physical negatives in #634 stay pending until independently
authorized and observed on the installed system.

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

This channel has no `/dispatch`, finance, account, installation, Research
lifecycle, or GitHub-control operations. Its disconnect does not close Engine
or Research runtime sockets.

## Disabled-by-default rollout

1. Complete the required source, Windows, Worker, and negative protocol CI.
   Do not infer a production `PASS` from CI fixtures alone.
2. Deploy the accepted Worker version with the independent
   `CONTROL_ATTEST_TOKEN_SHA256` fingerprint. It must be exactly 64 lowercase
   hex characters corresponding to a **new** CONTROL-only token generated from
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

Do not publish workstation identifiers, process paths, tokens, or live
fingerprints in public issue comments or pull-request logs.

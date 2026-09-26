# Encrypted GitHub mailbox protocol

This document defines the transport boundary for issue #7.

GitHub is a public mailbox, not a trusted plaintext data channel. All request and
response payloads are encrypted end-to-end.

## Roles

- **Agent**: native Windows `okx-agent.exe`, initially under `C:\okx`.
- **Client**: ChatGPT computation environment for one interactive request.
- **Mailbox**: public `iamaman11/okx` GitHub issues/comments.

The agent owns a long-lived X25519 key pair. Only its public key and key ID are
published. The client creates a fresh X25519 key pair for every request.

## Cryptographic construction

Version 1 uses:

- X25519 for the shared secret;
- HKDF-SHA256 for directional keys;
- ChaCha20-Poly1305 for authenticated encryption;
- a fresh 96-bit nonce for every encrypted payload.

Two independent keys are derived from the same shared secret:

```text
okx-mailbox-v1/client-to-agent
okx-mailbox-v1/agent-to-client
```

Implementations must not reuse one directional key for the opposite direction.

## Authenticated additional data

The exact version-1 AAD string is:

```text
schema=okx.mailbox.envelope/v1;repo=iamaman11/okx;request_id=<REQUEST_ID>;direction=<DIRECTION>;agent_key_id=<KEY_ID>
```

where `DIRECTION` is exactly `client_to_agent` or `agent_to_client`.

The AAD is UTF-8 without a trailing newline.

Any mismatch in repository identity, request ID, direction, schema or agent key
ID must make AEAD authentication fail or be rejected before decryption.

## Public request envelope

Only the envelope is written to GitHub:

```json
{
  "schema": "okx.mailbox.envelope/v1",
  "request_id": "req_0123456789abcdef",
  "direction": "client_to_agent",
  "agent_key_id": "agent-key-1",
  "client_ephemeral_public_key": "<base64>",
  "nonce": "<base64>",
  "ciphertext": "<base64>"
}
```

The response comment uses the same envelope schema with
`direction=agent_to_client` and the same request ID/client ephemeral public key.

## Plaintext request

After successful decryption, the agent accepts only the strongly typed
`okx.agent.request/v1` schema. Unknown operation types and unknown fields fail
closed.

No operation may represent an arbitrary shell command, arbitrary URL, source
code, generic HTTP request or generic file execution.

## Mailbox authorization

Cryptography does not replace GitHub identity checks. The agent must additionally
verify:

1. exact repository identity is `iamaman11/okx`;
2. issue author is explicitly allowlisted;
3. request ID has not already reached a terminal state;
4. requested agent key ID is accepted;
5. the encrypted envelope and decrypted typed request both validate.

## Privacy properties and limits

GitHub can still observe metadata: issue timing, ciphertext size, schema version,
request ID, public keys and key IDs. GitHub must not receive plaintext query
arguments, account state, positions, balances, risk results or OKX credentials.

The client ephemeral private key is request-scoped and must never be written to
GitHub, repository files, issue text, logs or plugin memory.

## Cross-language acceptance

Before live account data is enabled, Rust and the ChatGPT-side implementation
must pass the same fixed test vector containing:

- agent X25519 private/public key;
- client X25519 private/public key;
- request ID;
- agent key ID;
- AAD;
- shared secret;
- both HKDF outputs;
- nonce;
- plaintext;
- ciphertext.

The vector will be added when the cryptographic implementation lands. Protocol
types and the exact AAD/KDF labels are fixed by P0.

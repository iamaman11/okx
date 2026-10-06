# Native Windows agent runtime

Issue #7 P1 runs the observation backend natively on Windows.

## Runtime home

The operational target is:

```text
C:\okx
```

The Rust process is `okx-agent.exe`. WSL is not the runtime boundary.

## Secret boundary

The long-lived X25519 private key is not stored in the repository, a config file,
an environment variable, or command-line arguments.

On Windows, `okx-agent` stores the 32-byte private key in Windows Credential
Manager through the Rust keyring v1 API. The credential service name is:

```text
iamaman11.okx-agent
```

and the credential username is the agent `key_id` (initially
`agent-key-1`).

The `identity` command outputs only the key ID and X25519 public key.

## Commands

Initialize once:

```powershell
C:\okx\okx-agent.exe init-key
```

Inspect the publishable identity:

```powershell
C:\okx\okx-agent.exe identity
```

Run the lifecycle shell:

```powershell
C:\okx\okx-agent.exe run
```

P1 opens no inbound socket. The process reaches `READY_IDLE` and exits
gracefully on Ctrl+C.

For deterministic local transport acceptance, `once` reads one encrypted
mailbox envelope from a file or stdin, authenticates/decrypts it using the
Windows-stored agent key, validates the typed request, then returns an encrypted
terminal P1 response:

```powershell
C:\okx\okx-agent.exe once --input C:\okx\request-envelope.json
```

P1 intentionally returns `P1_OPERATION_NOT_AVAILABLE` after successfully
accepting the typed request. This proves the runtime/identity/crypto boundary
without pretending that the OKX observation domain is connected before P2.

## Invariants

- native Windows process;
- no WSL runtime dependency;
- no inbound network listener;
- no plaintext private key file;
- no private key printed to stdout/stderr;
- config/root path contains no secrets;
- typed encrypted requests only;
- unknown operations remain fail-closed in `okx-protocol`;
- OKX connectivity is deliberately absent from P1.

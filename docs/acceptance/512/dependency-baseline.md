# Dependency policy baseline — 2026-11-17

Status: **NOT ACCEPTED**. This is an audit finding record, not an exception list.

The existing locked graph was checked with cargo-deny 0.20.2, using RustSec
database commit `233155b4025b854c68a4b199c0e32d7fd53291a2`. The downloaded
scanner was checked against the official release checksum. The repository's
`deny.toml` has no advisory ignores and permits only reviewed registry sources.
CI retains the scanner report and database revision.

The scan rejected the current graph. Reported IDs:

```text
RUSTSEC-2026-0346 RUSTSEC-2026-0347 RUSTSEC-2026-0348
RUSTSEC-2026-0349 RUSTSEC-2026-0367 RUSTSEC-2026-0368
RUSTSEC-2026-0369 RUSTSEC-2026-0370 RUSTSEC-2026-0371
RUSTSEC-2026-0372 RUSTSEC-2026-0381 RUSTSEC-2026-0382
RUSTSEC-2026-0385 RUSTSEC-2026-0386 RUSTSEC-2026-0429
RUSTSEC-2026-0459 RUSTSEC-2026-0543 RUSTSEC-2026-0545
RUSTSEC-2026-0548
```

These findings include vulnerabilities and maintenance advisories; the count
is not a severity score. In particular the bundled `libsqlite3-sys 0.38.2`
reached through `rusqlite 0.40.2` requires review for the financial owner and
Research storage. The scan also reported the TLS/transport and cryptographic
dependency paths, unmaintained dependencies, and yanked versions. Consult the
full report for each affected version, dependency path, and remediation.

License policy rejected `CDLA-Permissive-2.0` on `webpki-roots 0.26.11` and
`webpki-roots 1.0.9`; it has not been silently added to the allowlist.

To close L06, resolve each blocking finding with a compatible targeted upgrade
and affected tests, or record an explicit scoped decision with owner, rationale,
and expiry. Review the actual license obligations before changing policy.
Changing unrelated crates or adding blanket exceptions is not remediation.
Functional Rust tests passing does not close this gate.

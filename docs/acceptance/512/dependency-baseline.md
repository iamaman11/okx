# Dependency policy — #512 P2

The initial #512 candidate passed the dependency policy with one scoped
maintenance decision on 2026-11-18. The subsequent chrono removal replaces
its six direct consumers with jiff 1.0 and removes that exception from
Cargo.toml and deny.toml; whole-workspace replacement CI is still required.
No final #512, installation or production-release acceptance is implied.

The refreshed RustSec DB is `233155b4025b854c68a4b199c0e32d7fd53291a2`.
The scanner remains cargo-deny 0.20.2; the verified Windows release archive
SHA-256 is `975a22143262fd27476d19ee00c7af67978426e40e1dee94eed6bbade1cf87dc`.
`advisories/licenses/sources` and the actual-graph maintenance scope guard pass.
The locked package count is reduced from 349 to 292. Reported vulnerabilities
were updated, unused unmaintained backends removed; chrono's maintenance notice
was initially handled explicitly rather than represented as fixed. The later
replacement eliminates that residual maintenance exception.

## Chrono removal and replacement verification — 2026-11-18

- Scope: all six direct Rust consumers in API, GitHub, Runtime, Agent,
  Host Control and Research Worker now use `jiff = "1.0"`. The resolved
  production lock contains `jiff 1.0.1`, no `chrono`, and no `iana-time-zone`.
  Workspace maintenance exceptions and cargo-deny advisory ignores are empty.
- Selection evidence: a temporary *independent workspace* compared the old
  Chrono output with `jiff 1.0.0` and `time 0.3.44`. Five behavior tests passed
  on each of Windows and Linux in
  [GitHub Actions run 44238027644](https://github.com/iamaman11/okx/actions/runs/44238027644):
  Unix-millisecond boundaries and fixed `.000Z` serialization, offsets and
  invalid RFC3339 input, three *calendar* months including month ends and leap
  years, Windows system-local scheduler formatting, and out-of-range rejection.
  `time` omitted `.000` at exact second boundaries by default; a fixed 90-day
  subtraction was not interchangeable with calendar months.
- Production graph verification: the full Linux workspace/all-target compilation
  of the resolved `jiff 1.0.1` graph passed in
  [run 44240009808](https://github.com/iamaman11/okx/actions/runs/44240009808).
  The existing dependency-policy job accepted the exact locked graph without
  the old advisory exception. The temporary comparison workflow and package
  were removed before the final PR gate.
- Unchanged invariants: a fixed millisecond RFC3339 UTC string is retained for
  OKX signing, finance archive cutoffs still subtract actual UTC calendar months,
  local autostart timestamps retain their 19-character Windows task format,
  and elapsed-time deadlines remain on `std::time::Instant`.
- This is verification of a code candidate, not installation or release
  authorization. Full Windows CI and human PR review remain separate gates.

Local boundary regression: execution 332 PASS / 2 pre-existing ignored
composition helpers; CONTROL 56 PASS; protocol 43 PASS / 1 pre-existing ignored
child; API 90 PASS; WS 15 PASS; Worker 94 PASS / 1 pre-existing ignored child
plus 7 actual IPC child tests; Windows credential adapter 2 PASS; Agent
research/activation/load 14 PASS and session transport 7 PASS. Ignored helpers
are not credited as physical acceptance. Workspace/all-target strict Clippy,
fmt, architecture and SQL guards pass. Fresh exact-commit financial acceptance
and canonical CI are recorded in #512 checkpoint comments.

## Original findings — 2026-11-17

The original locked graph was **NOT ACCEPTED**. The following is its historical
finding record; the explicit remediation and decisions follow below.

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

## Targeted remediation — 2026-11-18

| Original path | Candidate locked graph / change |
|---|---|
| rusqlite 0.40.2 / libsqlite3-sys 0.38.2 | All three owners use rusqlite =0.41.0 / bundled libsqlite3-sys 0.39.1; financial SQL schema and ledger rules unchanged |
| rustls 0.23.45 / webpki 0.103.15 | rustls 0.23.46 / rustls-webpki 0.103.17 |
| webpki-roots 1.0.9 | 1.0.12; 0.26.11 remains only its compatibility wrapper |
| ctutils 0.4.2 | 0.4.8 |
| Tokio 1.53.1 | 1.54.1 |
| ZIP 2.4.2 | Reviewed =9.4.2 with only deflate-flate2-zlib-rs; every open validates local/central consistency, reads enforce actual max+1 bounds |
| keyring 4.2 v1 / zbus / zvariant | Direct keyring-core + the same windows-native-keyring-store 1.1; no Unix/macOS secret-service wrapper graph, global store mutation or credential migration |
| yanked async-executor / yoke-derive | The former removed with the unused graph; the latter updated to 0.8.4. Yanked packages are now denied |

Check the final Cargo.lock and audit output for the authoritative versions.
The ZIP API change retains the existing manifest/hash/origin checks and does not
introduce a second installer. New tests use in-memory archives. Credential tests
use an injected mock store and local target construction only; they do not read
or write Windows Credential Manager. Existing service/user names and secret
encoding are preserved.

## Historical maintenance decision: RUSTSEC-2026-0543 (superseded by jiff migration)

- Review date: 2026-11-18. Implementation review: Codex in PR #513. Follow-up
  owner: repository maintainer, tracked in #512 L06. Maintainer acceptance is by
  normal PR review; this record does not invent prior human approval.
- Advisory classification: informational/unmaintained, not a reported specific
  vulnerability. The reviewed advisory lists all chrono versions and no patched
  upgrade. The currently locked version is 0.4.45.
- Scope: the existing six versioned direct consumers listed in deny.toml, across
  the unchanged locked Windows/Linux source graph. No new chrono consumer is
  admitted by this exception. Other advisories, including any future chrono
  vulnerability, remain errors.
- Decision proposed in this CODE candidate: retain the tested UTC/RFC3339,
  timestamp and freshness behavior temporarily instead of coupling a broad clock
  migration to SQLite/TLS/archive remediation. The residual maintenance risk is
  explicitly retained; chrono is not described as fixed.
- Absolute expiry: **2026-12-18**, enforced with the exact package version,
  actual dependent scope and advisory classification by
  `scripts/check_dependency_policy.py`. Pinned cargo-deny 0.20.2 has no native
  expiry/scope fields; workspace metadata is the explicit decision record.
  Before that date, review a bounded migration to a maintained time library or
  remove the exception if upstream circumstances change. No automatic extension.
- This was the bounded decision for the earlier candidate only. The subsequent
  migration removes the chrono dependency and this exception; the final lock
  and scanner report must confirm that no chrono consumer remains.
- Release permission is separate. A green policy result is not authorization
  to merge, deploy or trade.

Configuration reference: [cargo-deny advisory expiry and dependent allow list](https://embarkstudios.github.io/cargo-deny/checks/advisories/cfg.html).
All security and unsoundness advisories remain blocking; there is no global
unmaintained/source/license disable. Unused ignores are errors.

## Exact-package license review

The five original license files are retained byte-for-byte in [licenses](licenses).
Their copyright statements and conditions must accompany redistribution; this
source record is not a substitute for release materials.

- curve25519-dalek 5.0.0, subtle 2.6.1 and x25519-dalek 3.0.0: BSD-3-Clause.
  Retain their individual copyright/conditions/disclaimer in source and binary
  distribution materials; do not imply contributor endorsement. Only these
  versions receive package grants; the global license list is unchanged.
- webpki-roots 0.26.11 and 1.0.12: CDLA-Permissive-2.0 for certificate data.
  Make the agreement text available with redistributed data. Keep the copied
  license and source attribution with the release materials. No root trust or
  certificate verification rule is relaxed by the license decision.
- Follow-up owner: repository maintainer / #512 P3 release composition. Before
  publishing B1/B2 release artifacts, verify that those release materials make
  the retained notices available. P2 does not publish artifacts or install them.

Reviewed primary texts: [BSD-3-Clause](https://spdx.org/licenses/BSD-3-Clause),
[CDLA-Permissive-2.0](https://cdla.dev/permissive-2-0/).
Exact-package grants are in deny.toml; adding another license/package/version
still requires review.

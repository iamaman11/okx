# Architecture

## Phase 1 boundary

Phase 1 is intentionally read-only. It can authenticate and observe the account, but it cannot place, amend, cancel, or close orders.

The first runtime flow is:

```text
credentials from runtime environment
        |
        v
regional OKX REST endpoint
        |
        v
account/config
account/instruments
account/leverage-info
account/trade-fee
account/balance
account/positions
        |
        v
typed AccountCapabilities
```

## Invariants

- OKX is the source of truth for exchange and account state.
- Region and demo/production mode are explicit.
- Credentials never enter Git.
- Secret key, passphrase, and generated signatures are never serialized or logged.
- Instrument availability is queried from the authenticated account rather than inferred from geography.
- Margin mode is not a global position setting; future order placement must carry its explicit OKX trade mode.
- Trading endpoints are out of scope until the read-only acceptance gate passes.

## Regional endpoints

The runtime supports Global, EEA, and US/AU endpoint families. Demo uses the same regional REST base URL plus the OKX simulated-trading header and its dedicated WebSocket endpoint family.

## Next gate

After Phase 1 passes against a real read-only API key:

1. public/private WebSocket transport;
2. snapshot + event reconciliation;
3. demo-only execution;
4. live execution remains disabled until demo acceptance passes.

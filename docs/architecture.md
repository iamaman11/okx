# Architecture

## Account boundary

Production trading is isolated in the standard OKX sub-account `Succession`.

```text
OKX main account
  treasury / administrative authority
  no trading API used by runtime
        |
        v
Succession (standard sub-account)
  Futures mode
  long/short position mode
        |
        +-- observer key: Read only
        |
        +-- executor key: Read + Trade, never Withdraw
```

## Verified account invariants

The authenticated account acceptance must prove:

- Global production endpoint family.
- Standard sub-account (`uid != mainUid`, account type `1`).
- Futures account mode (`acctLv=2`).
- Long/short position mode (`posMode=long_short_mode`).
- Account STP fallback `cancel_maker`.
- Auto-loan disabled.
- Greeks display type `PA`.
- Observer key has `read_only` and has neither `trade` nor `withdraw`.
- Executor key has `read_only,trade` and never `withdraw`.
- Executor key must be IP-bound before production order submission is enabled.
- `ALLOW_LIVE_TRADING=false` is the Phase 1 invariant. Enabling it is a later execution-gate decision and never replaces exchange-side permissions or risk controls.

## Phase 1 boundary

Phase 1 authenticates and observes the real account but cannot place, amend, cancel, or close orders.

```text
credentials from selected runtime environment
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
- Region and production/demo mode are explicit.
- Credentials never enter Git.
- Secret key, passphrase, raw UID, and generated signatures are never serialized or logged.
- Instrument availability and contract metadata are queried from OKX rather than hardcoded.
- Margin mode is explicit per operation.
- Trading mutations will be owned by one Order Executor.
- No `Withdraw` credential exists for the runtime.
- A network-loss/unknown-ACK write must be reconciled by client order ID before any retry.
- Live writes remain impossible until the execution layer itself implements and passes its acceptance contract.

## UI-only preferences

These do not define the API contract and are not relied on by runtime correctness:

- chart/day boundary: 00:00 UTC;
- manual order confirmation: enabled;
- manual Close All confirmation: enabled;
- order-book click-to-fill amount: disabled;
- USDⓈ-margined futures trading unit: USDⓈ when the human wants dollar input;
- local display currency: USD.

## Next gate

Implement the execution layer with explicit `clOrdId`, order lifecycle/reconciliation, place/amend/cancel/close, risk guards, and unknown-submission recovery. Production order submission is accepted only after the executor credential and runtime guards satisfy the verified account contract.

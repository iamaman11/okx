# okx

Rust/Tokio trading infrastructure for OKX.

The project is being built in small, verifiable layers:

1. read-only account capabilities;
2. authenticated account state;
3. public/private WebSocket runtime and reconciliation;
4. demo order execution;
5. position supervision and logical positions;
6. risk/cost engine;
7. strategy and arbitrage engines;
8. guarded live trading.

## Phase 1: account capabilities

The first CLI is intentionally read-only. It asks OKX what the authenticated account can actually use instead of inferring capabilities from country alone.

It reports:

- account mode and position mode;
- available SWAP/FUTURES products;
- sample instruments visible to the account;
- maximum leverage advertised by the selected instrument;
- currently configured leverage for the selected margin mode;
- the instrument fee-group ID and account fee schedule;
- non-zero balance currency count and open-position count.

OKX fee rates are resolved through the instrument `groupId` / fee-group mapping. Current configured leverage and instrument maximum leverage are kept as two separate concepts.

### Credentials

Never commit credentials. Supply them at runtime:

```bash
export OKX_API_KEY='...'
export OKX_API_SECRET='...'
export OKX_API_PASSPHRASE='...'
```

For an EEA production account:

```bash
cargo run -p okx-cli --bin okx-capabilities -- \
  --region eea \
  --instrument BTC-USDT-SWAP \
  --margin cross
```

Use `--demo` only with an OKX Demo Trading API key.

## Security

- no API keys, secrets, or passphrases in Git;
- Phase 1 contains no place/amend/cancel/close-order API;
- no withdrawal or transfer API;
- secrets and generated signatures are not serialized or logged;
- live order execution will remain absent until read-only and demo acceptance gates pass.

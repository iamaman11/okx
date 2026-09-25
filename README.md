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

## Security

Never commit OKX API keys, secrets, or passphrases.

Credentials are supplied only at runtime. Live trading will remain disabled until the read-only and demo acceptance gates pass.

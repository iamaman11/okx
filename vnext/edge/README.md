# Edge workspace

The Worker, its public MCP schemas, OAuth and session transport checks can run
from this directory alone. Run `npm ci --ignore-scripts --no-audit --no-fund`,
`npm run check` and `node scripts/test-native-finance-retirement-do.mjs`.

The operator fixture is a snapshot of the accepted Research wire inputs at the
listed revision. The standalone suite verifies Edge against that snapshot.
The cross-owner compatibility suite must compare the next independently
versioned producer and consumer before introducing a new operator contract.
Native process integration and transfer of operator policy remain separate
migration work. The vNext workflow only checks and bundles; it has no deploy
step.

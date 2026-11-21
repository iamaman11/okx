# #518 T15 — independently extensible pure operator

**Scope:** CODE evidence for one new series primitive, not final T01–T15
acceptance, historical availability, an installed Windows release, or trading
permission. All market observations in the numeric example below are fixtures.

## Ownership / reviewed diff

| Existing owner | Change | No additional owner |
|---|---|---|
| `okx-kernel::query_plan` | `SeriesOp::RollingRange`, versioned `ROLLING_RANGE_DESCRIPTOR`, type/warmup/budget checks, fused O(n) deque implementation, causal null diagnostics and independent numeric oracle | No service, DSL interpreter or query router |
| `okx-research-worker::analytics::market` | Existing `market.screen` graph evaluator and capabilities consume the operator and its Rust descriptor; production planner/row test | No new source or persistence owner |
| `cloudflare/okx-mcp::execution_tools` | A new allowlisted enum value and strict period check in the existing `market` tool | No new MCP tool or routing-table entry |

**Unchanged:** financial SQLite schema, RiskBook, `TradeService` lifecycle,
accounts, order/venue mutations, Agent routing, IPC envelope, and existing
market scalar operator definitions. Missing external data is still reported by
the current source owners; an independently required venue/source/product
model needs its own adapter and provenance contract before exposure.

The operator returns `max(input[i-period+1..=i]) - min(...)`, inclusive of the
latest **closed** bar. It requires 2–1000 finite samples, preserves the unit
of a typed numeric price/ratio/contract/volume/coefficient series, propagates
any missing sample instead of turning it into zero, and produces a genuine 0
for a complete constant window. Two monotone deques cost amortized O(n) time
and O(period) extra state. A nonfinite arithmetic result remains unavailable.
The descriptor is declared in the kernel and serialized directly into Worker
`market.capabilities`: ID, semantic version, allowed input units, output shape
and unit, warmup, limits, null policy, time basis, effects and owner.

## Previously unregistered user combination, same binary

This is one valid public `market` request for the existing MCP route. Input
bars belong to one instrument and one strictly closed minute grid; public
source availability and rights still undergo their ordinary checks.

```json
{
  "action": "screen",
  "instruments": ["BTC-USDT"],
  "fields": ["returns"],
  "timeframe_ms": 60000,
  "start_ms": 60000,
  "end_ms": 6060000,
  "limit": 1,
  "composition": {
    "version": "okx.market.composition/v1",
    "steps": [
      {"id":"range_price","op":"rolling_range","input":"close","period":3},
      {"id":"wide","op":"greater_than","input":"range_price","threshold":5},
      {"id":"entry","op":"rising_edge","input":"wide"},
      {"id":"smooth","op":"rolling_mean","input":"range_price","period":2},
      {"id":"returns","op":"returns_series","input":"close"},
      {"id":"range_return","op":"rolling_range","input":"returns","period":2}
    ],
    "result_steps": ["range_price","wide","entry","smooth","range_return"]
  }
}
```

For the deterministic Worker test fixture (100 closes; 98 at 100, then 101,
110), the relevant part of the resulting row is:

```json
{
  "metrics": {
    "range_price": 10.0,
    "wide": true,
    "entry": true,
    "smooth": 5.5,
    "range_return": 0.07910891089108912
  },
  "units": {"range_price": "price", "range_return": "ratio"}
}
```

`metrics.range_return` is asserted using a floating-point tolerance, not a
byte-exact JSON oracle. Changing only `wide.threshold` to `20` in the request
makes `wide` and `entry` false while `range_price` remains 10. Neither
combination has a predefined strategy ID, or requires a rebuilt Worker.

## Direct reproducible evidence

- `okx-kernel::query_plan::t15_extension_contract`: independent O(period)
  numeric oracle over five window sizes and gaps; exact deterministic values;
  missing/NaN/overflow; type, future-label and bound rejection; prefix
  invariance; changing graph parameters through the same `compile` API.
- `okx-research-worker::analytics::market::tests::t15_range_extension_uses_existing_market_planner_and_user_composes_new_graph`: actual market plan and row projection, types, quality and two user graphs without a new route.
- `okx-research-worker::analytics::tests::market_capabilities_discloses_supported_operator_names_and_units`: Worker description exactly equals the kernel descriptor.
- `cloudflare/okx-mcp/src/execution_tools.test.ts`: public accepted graph and malformed period/parameter refusals.

These tests are **CODE evidence** once they pass on an exact SHA. No local
test fixture proves historical liquidity, archival permissions, external
venue support, installed runtime identity, or physical financial isolation.

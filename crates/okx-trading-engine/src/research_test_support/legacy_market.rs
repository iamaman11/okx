//! Historical Engine market witness used by the legacy Research fixtures.
use crate::{market_bootstrap::MarketBootstrapper, research_client::ResearchClient};
use jiff::Timestamp;
use okx_ipc_contracts::AgentOperation;
use okx_market_runtime::PublicWsHandle;
use serde_json::{Value, json};

pub(super) async fn execute(
    public_ws: &PublicWsHandle,
    market: &MarketBootstrapper,
    research: &ResearchClient,
    operation: &AgentOperation,
) -> Result<Value, String> {
    match operation {
        AgentOperation::Market { request } => execute_market(public_ws, research, request).await,
        AgentOperation::MarketCapabilities => research.capabilities().await,
        AgentOperation::MarketMicrostructure {
            instrument,
            size_contracts,
        } => execute_market_microstructure(public_ws, instrument, size_contracts.as_deref()).await,
        AgentOperation::FindInstruments {
            asset,
            settle_currency,
            instrument_type,
        } => {
            public_ws
                .ensure_reference_available(
                    std::time::Instant::now() + std::time::Duration::from_secs(5),
                )
                .await
                .map_err(|_| "REFERENCE_SOURCE_UNAVAILABLE")?;
            let refs = public_ws.reference_snapshot().await;
            Ok(find_instruments_response(
                &refs,
                asset,
                settle_currency.as_deref(),
                *instrument_type,
            ))
        }
        AgentOperation::MarketOverview { instrument } => {
            public_ws
                .ensure_reference_available(
                    std::time::Instant::now() + std::time::Duration::from_secs(5),
                )
                .await
                .map_err(|_| "REFERENCE_SOURCE_UNAVAILABLE")?;
            public_ws
                .demand_instrument(instrument)
                .await
                .map_err(|_| "MARKET_CAPACITY")?;
            let snapshot = public_ws
                .risk_fresh_snapshot(
                    instrument,
                    Timestamp::now().as_millisecond() as u64,
                    crate::config::QUOTE_MAX_AGE_MS,
                    crate::config::DERIVATIVE_CONTEXT_MAX_AGE_MS,
                    Timestamp::now().to_string(),
                )
                .await;
            match snapshot {
                Ok(snapshot) => Ok(compact_ws_overview(snapshot)),
                Err(_) => {
                    // A REST observation cannot certify a fresh WS book for trading.
                    // Expose bounded native WS quality without raw transport errors.
                    let ws_quality = match public_ws
                        .risk_quality_snapshot(
                            instrument,
                            Timestamp::now().as_millisecond().max(0) as u64,
                            crate::config::QUOTE_MAX_AGE_MS,
                            crate::config::DERIVATIVE_CONTEXT_MAX_AGE_MS,
                        )
                        .await
                    {
                        Ok(q) => json!({
                            "connection_state": q.connection_state,
                            "connection_generation": q.connection_generation,
                            "acknowledged_subscriptions": q.acknowledged_subscriptions,
                            "sequence_continuity_proven": q.sequence_continuity_proven,
                            "reason": q.reason,
                            "last_error_present": q.last_error.is_some()
                        }),
                        Err(_) => json!({"reason_code": "PUBLIC_WS_QUALITY_UNAVAILABLE"}),
                    };
                    let refs = public_ws.reference_snapshot().await;
                    let snapshot = market
                        .snapshot(&refs, instrument)
                        .await
                        .map_err(|_| "MARKET_SOURCE_UNAVAILABLE")?;
                    Ok(
                        json!({"schema":"okx.market.overview/v1","market":snapshot,"source_mode":"rest","quality":"degraded","reason":"PUBLIC_WS_WARMING","public_ws_quality":ws_quality}),
                    )
                }
            }
        }
        _ => unreachable!("legacy market fixture received a non-market operation"),
    }
}

/// A live market overview must not leak unbounded order-book depth into the
/// small public MCP envelope. Bounded book levels have their own microstructure action.
fn compact_ws_overview(snapshot: okx_observation::LiveMarketSnapshot) -> Value {
    json!({
        "schema": "okx.market.overview/v1",
        "source_mode": "websocket",
        "quality": "complete",
        "market": snapshot.market,
        "generation": snapshot.generation,
        "book_sequence": snapshot.order_book.seq_id,
        "book_exchange_timestamp_ms": snapshot.order_book.exchange_timestamp_ms
    })
}

fn find_instruments_response(
    reference: &okx_observation::ReferenceRegistry,
    asset: &str,
    settle_currency: Option<&str>,
    instrument_type: Option<okx_ipc_contracts::InstrumentTypeFilter>,
) -> Value {
    let instrument_type = instrument_type.map(|kind| match kind {
        okx_ipc_contracts::InstrumentTypeFilter::Swap => okx_observation::InstrumentType::Swap,
        okx_ipc_contracts::InstrumentTypeFilter::Futures => {
            okx_observation::InstrumentType::Futures
        }
    });
    let mut search = reference.find_instruments(asset, settle_currency, instrument_type);
    search.instruments.truncate(50);
    json!({
        "instruments": search.instruments,
        "reference_generation": search.reference_generation
    })
}

/// Resolve an optional bounded instrument universe, then delegate market work
/// to the isolated Research Worker. Kept outside the command dispatcher so routing stays
/// visible and this market-specific boundary can be reviewed independently.
async fn execute_market(
    public_ws: &PublicWsHandle,
    research: &ResearchClient,
    request: &okx_ipc_contracts::analytics::MarketRequest,
) -> Result<Value, String> {
    let mut universe_page = None;
    let mut resolved_instruments = None;
    if let Some(universe) = &request.universe {
        public_ws
            .ensure_reference_available(
                std::time::Instant::now() + std::time::Duration::from_secs(5),
            )
            .await
            .map_err(|_| "REFERENCE_SOURCE_UNAVAILABLE")?;
        let reference = public_ws.reference_snapshot().await;
        if universe
            .reference_generation
            .as_deref()
            .is_some_and(|expected| expected != reference.generation().as_str())
        {
            return Err("UNIVERSE_REFERENCE_GENERATION_CHANGED".into());
        }
        let mut names = reference
            .iter()
            .filter(|spec| spec.state == "live")
            .filter(|spec| {
                format!("{:?}", spec.instrument_type).to_uppercase() == universe.instrument_type
            })
            .filter(|spec| {
                universe
                    .settle_currency
                    .as_ref()
                    .is_none_or(|settle| spec.settle_currency.as_ref() == Some(settle))
            })
            .map(|spec| spec.instrument_id.clone())
            .collect::<Vec<_>>();
        names.sort_unstable();
        names.dedup();
        let universe_size = names.len();
        if universe.rank_globally && names.len() > 512 {
            return Err("GLOBAL_RANK_UNIVERSE_LIMIT:512".into());
        }
        if !universe.rank_globally {
            if let Some(after) = &universe.after_instrument {
                names.retain(|name| name > after);
            }
            names.truncate(usize::from(universe.limit).saturating_add(1));
        }
        let has_more = if universe.rank_globally {
            names.len() > usize::from(request.limit)
        } else {
            names.len() > usize::from(universe.limit)
        };
        if !universe.rank_globally {
            names.truncate(usize::from(universe.limit));
        }
        if names.is_empty() {
            return Err("UNIVERSE_PAGE_EMPTY".into());
        }
        universe_page = Some(json!({
            "schema":"okx.market.universe-page/v1",
            "source":"venue_reference_registry",
            "reference_generation":reference.generation().as_str(),
            "reference_received_at":reference.source_received_at(),
            "global_ranking":universe.rank_globally,
            "scope":if universe.rank_globally {"complete_venue_universe"} else {"bounded_market_page"},
            "has_more":has_more,
            "next_after_instrument":if has_more && !universe.rank_globally {names.last().cloned()}else{None},
            "universe_size":universe_size,
            "page_count":if universe.rank_globally {names.len().min(usize::from(request.limit))}else{names.len()}
        }));
        resolved_instruments = Some(names);
    }
    research
        .market(request.clone(), universe_page, resolved_instruments)
        .await
}

/// Read a fresh contiguous public WS book and calculate its bounded
/// microstructure summary. No REST fallback can certify a trading book.
async fn execute_market_microstructure(
    public_ws: &PublicWsHandle,
    instrument: &str,
    size_contracts: Option<&str>,
) -> Result<Value, String> {
    public_ws
        .demand_instrument(instrument)
        .await
        .map_err(|_| "MARKET_CAPACITY")?;
    let snap = public_ws
        .risk_fresh_snapshot(
            instrument,
            Timestamp::now().as_millisecond().max(0) as u64,
            crate::config::QUOTE_MAX_AGE_MS,
            crate::config::DERIVATIVE_CONTEXT_MAX_AGE_MS,
            Timestamp::now().to_string(),
        )
        .await
        .map_err(|_| "MICROSTRUCTURE_FRESH_BOOK_REQUIRED")?;
    microstructure_summary(snap, instrument, size_contracts)
}

/// Pure response calculation after the WS owner has admitted the snapshot.
/// It cannot fetch data, access accounts or send an order.
fn microstructure_summary(
    snap: okx_observation::LiveMarketSnapshot,
    instrument: &str,
    size_contracts: Option<&str>,
) -> Result<Value, String> {
    let asks = snap
        .order_book
        .asks
        .iter()
        .take(10)
        .cloned()
        .collect::<Vec<_>>();
    let bids = snap
        .order_book
        .bids
        .iter()
        .take(10)
        .cloned()
        .collect::<Vec<_>>();
    let best_ask = asks
        .first()
        .ok_or("BOOK_ASK_EMPTY")?
        .price
        .parse::<rust_decimal::Decimal>()
        .map_err(|_| "BOOK_PRICE_INVALID")?;
    let best_bid = bids
        .first()
        .ok_or("BOOK_BID_EMPTY")?
        .price
        .parse::<rust_decimal::Decimal>()
        .map_err(|_| "BOOK_PRICE_INVALID")?;
    if best_bid <= rust_decimal::Decimal::ZERO || best_ask <= best_bid {
        return Err("BOOK_SPREAD_INVALID".into());
    }
    let spread =
        (best_ask - best_bid) * rust_decimal::Decimal::new(20_000, 0) / (best_ask + best_bid);
    let sweep = size_contracts
        .map(|n| {
            let qty = n
                .parse::<rust_decimal::Decimal>()
                .map_err(|_| "SIZE_INVALID")?;
            let buy = book_sweep_vwap(&asks, qty)?;
            let sell = book_sweep_vwap(&bids, qty)?;
            Ok::<_, String>(json!({
                "size_contracts":n,
                "buy_vwap":buy.normalize().to_string(),
                "sell_vwap":sell.normalize().to_string(),
                "buy_impact_bps":((buy-best_ask)*rust_decimal::Decimal::new(10_000,0)/best_ask).normalize().to_string(),
                "sell_impact_bps":((best_bid-sell)*rust_decimal::Decimal::new(10_000,0)/best_bid).normalize().to_string()
            }))
        })
        .transpose()?;
    Ok(json!({
        "schema":"okx.market.microstructure/v1",
        "instrument":instrument,
        "quality":"complete",
        "reference_generation":snap.market.reference_generation,
        "generation":snap.generation,
        "book_sequence":snap.order_book.seq_id,
        "book_exchange_timestamp_ms":snap.order_book.exchange_timestamp_ms,
        "spread_bps":spread.normalize().to_string(),
        "asks":asks,
        "bids":bids,
        "sweep":sweep,
        "mark_price":snap.market.mark_price,
        "index_price":snap.market.index_price,
        "funding":snap.market.funding,
        "open_interest":snap.market.open_interest
    }))
}

fn book_sweep_vwap(
    levels: &[okx_observation::BookLevel],
    requested: rust_decimal::Decimal,
) -> Result<rust_decimal::Decimal, String> {
    use rust_decimal::Decimal;
    if requested <= Decimal::ZERO {
        return Err("SIZE_INVALID".into());
    }
    let mut filled = Decimal::ZERO;
    let mut cost = Decimal::ZERO;
    for level in levels {
        let price = level
            .price
            .parse::<Decimal>()
            .map_err(|_| "BOOK_PRICE_INVALID")?;
        let size = level
            .size
            .parse::<Decimal>()
            .map_err(|_| "BOOK_SIZE_INVALID")?;
        if price <= Decimal::ZERO || size < Decimal::ZERO {
            return Err("BOOK_LEVEL_INVALID".into());
        }
        let take = (requested - filled).min(size);
        filled = filled.checked_add(take).ok_or("BOOK_OVERFLOW")?;
        cost = cost
            .checked_add(price.checked_mul(take).ok_or("BOOK_OVERFLOW")?)
            .ok_or("BOOK_OVERFLOW")?;
        if filled >= requested {
            return cost.checked_div(filled).ok_or("BOOK_OVERFLOW".into());
        }
    }
    Err("INSUFFICIENT_BOOK_DEPTH".into())
}
#[cfg(test)]
mod instrument_search_tests {
    use super::*;
    use okx_ipc_contracts::InstrumentTypeFilter;

    fn instrument(id: &str, kind: &str, asset: &str, currency: &str) -> okx_api::PublicInstrument {
        serde_json::from_value(json!({
            "instId": id,
            "instType": kind,
            "state": "live",
            "baseCcy": if kind == "SWAP" { asset } else { "" },
            "ctValCcy": asset,
            "settleCcy": currency,
            "tickSz": "0.1",
            "lotSz": "1",
            "minSz": "1"
        }))
        .unwrap()
    }

    #[test]
    fn find_instruments_filters_type_currency_and_asset_before_public_limit() {
        // All sixty swaps sort before the requested futures, so filtering only
        // after the public row limit would return no matching instruments.
        let mut instruments = (0..60)
            .map(|i| instrument(&format!("A-{i:02}"), "SWAP", "BTC", "USDT"))
            .collect::<Vec<_>>();
        instruments.push(instrument("B-USDC", "FUTURES", "BTC", "USDC"));
        instruments.push(instrument("C-ETH", "FUTURES", "ETH", "USDT"));
        instruments
            .extend((0..55).map(|i| instrument(&format!("Z-{i:02}"), "FUTURES", "BTC", "USDT")));
        let reference =
            okx_observation::ReferenceRegistry::from_public("fixture", instruments).unwrap();

        let futures = find_instruments_response(
            &reference,
            "BTC",
            Some("USDT"),
            Some(InstrumentTypeFilter::Futures),
        );
        let rows = futures["instruments"].as_array().unwrap();
        assert_eq!(rows.len(), 50);
        assert_eq!(rows[0]["instrument_id"], "Z-00");
        assert_eq!(rows[49]["instrument_id"], "Z-49");
        assert!(rows.iter().all(|row| row["instrument_type"] == "FUTURES"
            && row["settle_currency"] == "USDT"
            && row["contract_value_currency"] == "BTC"));
        assert_eq!(futures.as_object().unwrap().len(), 2);
        assert_eq!(
            futures["reference_generation"],
            reference.generation().as_str()
        );

        let swaps = find_instruments_response(
            &reference,
            "BTC",
            Some("USDT"),
            Some(InstrumentTypeFilter::Swap),
        );
        assert_eq!(swaps["instruments"].as_array().unwrap().len(), 50);
        assert_eq!(swaps["instruments"][0]["instrument_id"], "A-00");
        assert!(
            swaps["instruments"]
                .as_array()
                .unwrap()
                .iter()
                .all(|row| row["instrument_type"] == "SWAP")
        );

        for (asset, currency, expected) in [("BTC", "USDC", "B-USDC"), ("ETH", "USDT", "C-ETH")] {
            let result = find_instruments_response(&reference, asset, Some(currency), None);
            assert_eq!(result["instruments"].as_array().unwrap().len(), 1);
            assert_eq!(result["instruments"][0]["instrument_id"], expected);
        }
        let unfiltered = find_instruments_response(&reference, "BTC", None, None);
        assert_eq!(unfiltered["instruments"].as_array().unwrap().len(), 50);
    }
}

#[cfg(test)]
mod microstructure_test {
    use super::*;

    fn market_snapshot() -> okx_observation::LiveMarketSnapshot {
        let market = okx_observation::MarketSnapshot {
            schema: "okx.market-snapshot/v1".into(),
            instrument_id: "BTC-USDT-SWAP".into(),
            instrument_type: okx_observation::InstrumentType::Swap,
            underlying: "BTC-USDT".into(),
            reference_generation: "reference".into(),
            market_generation: "market".into(),
            source_received_at: "2026-10-24T02:00:00Z".into(),
            ticker: okx_observation::TickerState {
                last: "100".into(),
                last_size: "1".into(),
                best_ask: "100.1".into(),
                best_ask_size: "2".into(),
                best_bid: "100".into(),
                best_bid_size: "3".into(),
                open_24h: None,
                high_24h: None,
                low_24h: None,
                volume_currency_24h: None,
                volume_24h: None,
                exchange_timestamp_ms: "1000".into(),
            },
            mark_price: okx_observation::MarkPriceState {
                price: "100".into(),
                exchange_timestamp_ms: "1000".into(),
            },
            index_price: okx_observation::IndexPriceState {
                index_id: "BTC-USDT".into(),
                price: "100".into(),
                exchange_timestamp_ms: "1000".into(),
            },
            funding: None,
            open_interest: okx_observation::OpenInterestState {
                contracts: "100".into(),
                currency: None,
                usd: None,
                exchange_timestamp_ms: "1000".into(),
            },
        };
        let depth = okx_observation::BookLevel {
            price: "100".into(),
            size: "1".into(),
            order_count: None,
        };
        okx_observation::LiveMarketSnapshot {
            generation: 7,
            market,
            order_book: okx_observation::OrderBookSnapshot {
                generation: 7,
                status: okx_observation::OrderBookStatus::Contiguous,
                seq_id: Some(123),
                exchange_timestamp_ms: Some("1000".into()),
                asks: vec![depth.clone(); 2000],
                bids: vec![depth; 2000],
            },
        }
    }

    #[test]
    fn healthy_ws_overview_omits_unbounded_depth_and_fits_mcp_envelope() {
        let projected = compact_ws_overview(market_snapshot());
        assert_eq!(projected["source_mode"], "websocket");
        assert_eq!(projected["market"]["ticker"]["best_bid"], "100");
        assert_eq!(projected["book_sequence"], 123);
        assert!(projected.get("order_book").is_none());
        assert!(serde_json::to_vec(&projected).unwrap().len() < 12_288);
    }

    #[test]
    fn microstructure_projects_exact_spread_and_sweep_without_io() {
        let mut snapshot = market_snapshot();
        snapshot.order_book.asks = vec![okx_observation::BookLevel {
            price: "102".into(),
            size: "2".into(),
            order_count: None,
        }];
        snapshot.order_book.bids = vec![okx_observation::BookLevel {
            price: "98".into(),
            size: "2".into(),
            order_count: None,
        }];
        let result = microstructure_summary(snapshot, "BTC-USDT-SWAP", Some("1")).unwrap();
        assert_eq!(result["spread_bps"], "400");
        assert_eq!(result["sweep"]["buy_vwap"], "102");
        assert_eq!(result["sweep"]["sell_vwap"], "98");
        assert_eq!(result["sweep"]["buy_impact_bps"], "0");
        assert_eq!(result["book_sequence"], 123);
    }

    #[test]
    fn microstructure_rejects_crossed_book_before_sizing() {
        assert_eq!(
            microstructure_summary(market_snapshot(), "BTC-USDT-SWAP", Some("invalid"))
                .unwrap_err(),
            "BOOK_SPREAD_INVALID"
        );
    }

    #[test]
    fn market_local_budget_failure_is_addressable_and_retryable() {
        let r = crate::query::response(
            "market-defer",
            Err("OKX rate/backpressure defer: MARKET_SOURCE_RETRYABLE".into()),
        );
        let failure = r.failure.unwrap();
        assert_eq!(failure.code, "MARKET_SOURCE_RETRYABLE");
        assert!(failure.retryable);
        let fatal = crate::query::response("market-fatal", Err("CANDLE_INTERIOR_GAP".into()));
        assert!(!fatal.failure.unwrap().retryable);
    }
    #[test]
    fn bounded_book_sweep_rejects_insufficient_depth() {
        let levels = vec![
            okx_observation::BookLevel {
                price: "100".into(),
                size: "1".into(),
                order_count: None,
            },
            okx_observation::BookLevel {
                price: "102".into(),
                size: "1".into(),
                order_count: None,
            },
        ];
        let qty = rust_decimal::Decimal::new(2, 0);
        assert_eq!(
            book_sweep_vwap(&levels, qty).unwrap(),
            rust_decimal::Decimal::new(101, 0)
        );
        assert!(book_sweep_vwap(&levels, rust_decimal::Decimal::new(3, 0)).is_err());
    }
}

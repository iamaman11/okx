//! Product dispatcher; explicit account routing and bounded native responses.
use crate::{
    accounts::AccountRuntimeRegistry, analytics::AnalyticsService, trade_service::TradeService,
};
use chrono::Utc;
use okx_protocol::{
    AGENT_RESPONSE_SCHEMA_V1, AgentFailure, AgentOperation, AgentRequest, AgentResponse,
    AgentResponseStatus, DataQuality, account_governance::CentralGovernanceSnapshot,
};
use okx_runtime::PublicWsHandle;
use serde_json::{Value, json};
use std::sync::Arc;
#[derive(Clone)]
pub struct DispatchContext {
    pub accounts: Arc<AccountRuntimeRegistry>,
    pub analytics: AnalyticsService,
    pub public_ws: PublicWsHandle,
    pub trade: Arc<TradeService>,
    pub market: crate::market_bootstrap::MarketBootstrapper,
}
pub async fn dispatch(ctx: &DispatchContext, r: &AgentRequest) -> AgentResponse {
    dispatch_with_governance(ctx, r, None).await
}
pub async fn dispatch_with_governance(
    ctx: &DispatchContext,
    r: &AgentRequest,
    g: Option<&CentralGovernanceSnapshot>,
) -> AgentResponse {
    let result = match r.validate() {
        Ok(()) => execute(ctx, r, g).await,
        Err(_) => Err("INVALID_COMMAND".into()),
    };
    response(&r.request_id, result)
}
fn incomplete(v: &Value) -> bool {
    match v {
        Value::Object(fields) => {
            fields
                .get("quality")
                .and_then(Value::as_str)
                .is_some_and(|q| matches!(q, "partial" | "missing" | "stale" | "degraded"))
                || fields.values().any(incomplete)
        }
        Value::Array(rows) => rows.iter().any(incomplete),
        _ => false,
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

pub fn response(id: &str, result: Result<Value, String>) -> AgentResponse {
    let quality = match &result {
        Ok(value) if incomplete(value) => DataQuality::Degraded,
        Ok(_) => DataQuality::Fresh,
        Err(_) => DataQuality::NotReady,
    };
    let (status, data, failure) = match result {
        Ok(v) => (AgentResponseStatus::Completed, Some(v), None),
        Err(code) => {
            let retryable = crate::analytics::is_retryable_market_source_defer(&code);
            let public_code = if retryable {
                "MARKET_SOURCE_RETRYABLE".to_owned()
            } else {
                code
            };
            (
                AgentResponseStatus::Rejected,
                None,
                Some(AgentFailure {
                    message: public_code.clone(),
                    code: public_code,
                    retryable,
                }),
            )
        }
    };
    AgentResponse {
        schema: AGENT_RESPONSE_SCHEMA_V1.into(),
        request_id: id.into(),
        status,
        generated_at: Utc::now().to_rfc3339(),
        quality,
        result_schema: Some("okx.product/v1".into()),
        result: data,
        failure,
        warnings: vec![],
    }
}
async fn execute(
    ctx: &DispatchContext,
    r: &AgentRequest,
    g: Option<&CentralGovernanceSnapshot>,
) -> Result<Value, String> {
    let account = r.account.as_deref();
    if r.operation.requires_account() && account.is_none() {
        return Err("EXPLICIT_ACCOUNT_REQUIRED".into());
    }
    match &r.operation {
        AgentOperation::ConnectAccount {
            credential_ref,
            alias,
            operation_id,
            request_digest,
        } => {
            let c = ctx
                .accounts
                .connect(credential_ref, alias, operation_id, request_digest)
                .await
                .map_err(|e| e.to_string())?;
            serde_json::to_value(c).map_err(|_| "SERIALIZATION_FAILED".into())
        }
        AgentOperation::Market { request } => {
            let mut resolved = request.clone();
            let mut universe_page = None;
            if let Some(universe) = &request.universe {
                let reference = ctx.public_ws.reference_snapshot().await;
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
                        format!("{:?}", spec.instrument_type).to_uppercase()
                            == universe.instrument_type
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
                if let Some(after) = &universe.after_instrument {
                    names.retain(|name| name > after);
                }
                let has_more = names.len() > usize::from(universe.limit);
                names.truncate(usize::from(universe.limit));
                if names.is_empty() {
                    return Err("UNIVERSE_PAGE_EMPTY".into());
                }
                universe_page = Some(json!({
                    "schema":"okx.market.universe-page/v1",
                    "source":"venue_reference_registry",
                    "reference_generation":reference.generation().as_str(),
                    "reference_received_at":reference.source_received_at(),
                    "global_ranking":false,"scope":"bounded_market_page",
                    "has_more":has_more,
                    "next_after_instrument":if has_more {names.last().cloned()}else{None},
                    "universe_size":universe_size,
                    "page_count":names.len()
                }));
                resolved.instruments = names;
                resolved.universe = None;
            }
            ctx.analytics.market(resolved, universe_page).await
        }
        AgentOperation::MarketCapabilities => Ok(ctx.analytics.market_capabilities()),
        AgentOperation::MarketMicrostructure {
            instrument,
            size_contracts,
        } => {
            ctx.public_ws
                .demand_instrument(instrument)
                .await
                .map_err(|_| "MARKET_CAPACITY")?;
            // Fail closed on any stale or sequence-discontinuous order book: no REST fallback.
            let snap = ctx
                .public_ws
                .risk_fresh_snapshot(
                    instrument,
                    Utc::now().timestamp_millis().max(0) as u64,
                    crate::config::QUOTE_MAX_AGE_MS,
                    crate::config::DERIVATIVE_CONTEXT_MAX_AGE_MS,
                    Utc::now().to_rfc3339(),
                )
                .await
                .map_err(|_| "MICROSTRUCTURE_FRESH_BOOK_REQUIRED")?;
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
            let spread = (best_ask - best_bid) * rust_decimal::Decimal::new(20_000, 0)
                / (best_ask + best_bid);
            let sweep=size_contracts.as_ref().map(|n|{
                let qty=n.parse::<rust_decimal::Decimal>().map_err(|_|"SIZE_INVALID")?;
                let buy=book_sweep_vwap(&asks,qty)?;
                let sell=book_sweep_vwap(&bids,qty)?;
                Ok::<_,String>(json!({"size_contracts":n,"buy_vwap":buy.normalize().to_string(),
                    "sell_vwap":sell.normalize().to_string(),
                    "buy_impact_bps":((buy-best_ask)*rust_decimal::Decimal::new(10_000,0)/best_ask).normalize().to_string(),
                    "sell_impact_bps":((best_bid-sell)*rust_decimal::Decimal::new(10_000,0)/best_bid).normalize().to_string()}))
            }).transpose()?;
            Ok(json!({"schema":"okx.market.microstructure/v1",
                "instrument":instrument,"quality":"complete",
                "reference_generation":snap.market.reference_generation,
                "generation":snap.generation,"book_sequence":snap.order_book.seq_id,
                "book_exchange_timestamp_ms":snap.order_book.exchange_timestamp_ms,
                "spread_bps":spread.normalize().to_string(),"asks":asks,"bids":bids,
                "sweep":sweep,"mark_price":snap.market.mark_price,
                "index_price":snap.market.index_price,
                "funding":snap.market.funding,"open_interest":snap.market.open_interest}))
        }
        AgentOperation::Research { request } => ctx.analytics.research(request.clone()).await,
        AgentOperation::Trade { request } => {
            ctx.trade
                .trade(account.unwrap_or_default(), request, g)
                .await
        }
        AgentOperation::Strategies { request } => {
            ctx.trade
                .strategies(account.unwrap_or_default(), request, g)
                .await
        }
        AgentOperation::PortfolioScenario { request } => crate::portfolio::scenario(request),
        AgentOperation::Portfolio { request } => {
            crate::portfolio::portfolio(&ctx.accounts, request, &ctx.trade).await
        }
        AgentOperation::Diagnostics { request } => {
            crate::diagnostics::diagnostics(ctx, request, account).await
        }
        AgentOperation::AccountSummary => {
            let a = ctx
                .accounts
                .resolve(account.unwrap_or_default())
                .await
                .ok_or("ACCOUNT_UNAVAILABLE")?;
            let snapshot = a
                .latest_snapshot
                .read()
                .await
                .clone()
                .ok_or("WAITING_ACCOUNT_EVIDENCE")?;
            let observed = chrono::DateTime::parse_from_rfc3339(&snapshot.source_received_at)
                .map_err(|_| "ACCOUNT_SOURCE_TIME_INVALID")?
                .timestamp_millis();
            let now = Utc::now().timestamp_millis();
            if !crate::config::evidence_fresh(observed, now, crate::config::ACCOUNT_MAX_AGE_MS) {
                return Err("WAITING_ACCOUNT_EVIDENCE".into());
            }
            Ok(
                json!({"account":a.configuration.account_key.wire(),"source_received_at":snapshot.source_received_at,"total_equity_usd":snapshot.balance.total_equity_usd,"initial_margin_usd":snapshot.balance.initial_margin_requirement_usd,"notional_usd":snapshot.balance.notional_usd,"position_count":snapshot.positions.len(),"pending_order_count":snapshot.pending_orders.len(),"currencies":snapshot.balance.details.iter().take(16).map(|d|json!({"currency":d.currency,"equity":d.equity,"available":d.available_equity})).collect::<Vec<_>>() }),
            )
        }
        AgentOperation::FindInstruments {
            asset,
            settle_currency,
            instrument_type: _,
        } => {
            let refs = ctx.public_ws.reference_snapshot().await;
            let rows = refs
                .iter()
                .filter(|x| {
                    x.base_currency.as_deref() == Some(asset)
                        || x.contract_value_currency.as_deref() == Some(asset)
                })
                .filter(|x| {
                    settle_currency
                        .as_ref()
                        .is_none_or(|c| x.settle_currency.as_ref() == Some(c))
                })
                .take(50)
                .collect::<Vec<_>>();
            Ok(json!({"instruments":rows,"reference_generation":refs.generation().as_str()}))
        }
        AgentOperation::MarketOverview { instrument } => {
            ctx.public_ws
                .demand_instrument(instrument)
                .await
                .map_err(|_| "MARKET_CAPACITY")?;
            let snapshot = ctx
                .public_ws
                .risk_fresh_snapshot(
                    instrument,
                    Utc::now().timestamp_millis() as u64,
                    crate::config::QUOTE_MAX_AGE_MS,
                    crate::config::DERIVATIVE_CONTEXT_MAX_AGE_MS,
                    Utc::now().to_rfc3339(),
                )
                .await;
            match snapshot {
                Ok(snapshot) => Ok(compact_ws_overview(snapshot)),
                Err(_) => {
                    // A REST observation cannot certify a fresh WS book for trading.
                    // Expose bounded native WS quality without raw transport errors.
                    let ws_quality = match ctx
                        .public_ws
                        .risk_quality_snapshot(
                            instrument,
                            Utc::now().timestamp_millis().max(0) as u64,
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
                    let refs = ctx.public_ws.reference_snapshot().await;
                    let snapshot = ctx
                        .market
                        .snapshot(&refs, instrument)
                        .await
                        .map_err(|_| "MARKET_SOURCE_UNAVAILABLE")?;
                    Ok(
                        json!({"schema":"okx.market.overview/v1","market":snapshot,"source_mode":"rest","quality":"degraded","reason":"PUBLIC_WS_WARMING","public_ws_quality":ws_quality}),
                    )
                }
            }
        }
        AgentOperation::MailboxTelemetry => Ok(
            json!({"contract":"okx.product/v1","installation_id":ctx.accounts.installation_id()}),
        ),
    }
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
mod microstructure_test {
    use super::*;
    #[test]
    fn market_local_budget_failure_is_addressable_and_retryable() {
        let r = response(
            "market-defer",
            Err("OKX rate/backpressure defer: MARKET_SOURCE_RETRYABLE".into()),
        );
        let failure = r.failure.unwrap();
        assert_eq!(failure.code, "MARKET_SOURCE_RETRYABLE");
        assert!(failure.retryable);
        let fatal = response("market-fatal", Err("CANDLE_INTERIOR_GAP".into()));
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

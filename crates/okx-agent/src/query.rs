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
pub fn response(id: &str, result: Result<Value, String>) -> AgentResponse {
    let quality = match &result {
        Ok(value) if incomplete(value) => DataQuality::Degraded,
        Ok(_) => DataQuality::Fresh,
        Err(_) => DataQuality::NotReady,
    };
    let (status, data, failure) = match result {
        Ok(v) => (AgentResponseStatus::Completed, Some(v), None),
        Err(code) => (
            AgentResponseStatus::Rejected,
            None,
            Some(AgentFailure {
                message: code.clone(),
                code,
                retryable: false,
            }),
        ),
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
        AgentOperation::Market { request } => ctx.analytics.market(request.clone()).await,
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
                .fresh_snapshot(
                    instrument,
                    Utc::now().timestamp_millis() as u64,
                    crate::config::QUOTE_MAX_AGE_MS,
                    Utc::now().to_rfc3339(),
                )
                .await;
            match snapshot {
                Ok(snapshot) => {
                    serde_json::to_value(snapshot).map_err(|_| "SERIALIZATION_FAILED".into())
                }
                Err(_) => {
                    let refs = ctx.public_ws.reference_snapshot().await;
                    let snapshot = ctx
                        .market
                        .snapshot(&refs, instrument)
                        .await
                        .map_err(|_| "MARKET_SOURCE_UNAVAILABLE")?;
                    Ok(
                        json!({"market":snapshot,"source_mode":"rest","quality":"degraded","reason":"PUBLIC_WS_WARMING"}),
                    )
                }
            }
        }
        AgentOperation::MailboxTelemetry => Ok(
            json!({"contract":"okx.product/v1","installation_id":ctx.accounts.installation_id()}),
        ),
    }
}

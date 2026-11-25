//! Product dispatcher; explicit account routing and bounded native responses.
use crate::finance_dispatch::FinanceDispatch;
use jiff::Timestamp;
use okx_ipc_contracts::{
    AGENT_RESPONSE_SCHEMA_V1, AgentFailure, AgentOperation, AgentRequest, AgentResponse,
    AgentResponseStatus, DataQuality, account_governance::CentralGovernanceSnapshot,
    trading::DiagnosticsView,
};
use serde_json::{Value, json};
use std::str::FromStr;
use tracing::Instrument;
#[derive(Clone)]
pub struct DispatchContext {
    pub finance: FinanceDispatch,
}
#[cfg(test)]
#[path = "query_dispatch_tests.rs"]
mod public_dispatch_tests;
pub async fn dispatch(ctx: &DispatchContext, r: &AgentRequest) -> AgentResponse {
    dispatch_with_governance(ctx, r, None).await
}
pub async fn dispatch_with_governance(
    ctx: &DispatchContext,
    r: &AgentRequest,
    g: Option<&CentralGovernanceSnapshot>,
) -> AgentResponse {
    let span = tracing::info_span!(
        "product_invocation",
        invocation_id = r.request_id.as_str(),
        owner = "agent_dispatch"
    );
    let result = match r.validate() {
        Ok(()) => execute(ctx, r, g).instrument(span).await,
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
        Err(code) => {
            let retryable = code.contains("OKX rate/backpressure defer:");
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
        generated_at: Timestamp::now().to_string(),
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
                .finance
                .accounts()?
                .connect(credential_ref, alias, operation_id, request_digest)
                .await
                .map_err(|e| e.to_string())?;
            serde_json::to_value(c).map_err(|_| "SERIALIZATION_FAILED".into())
        }
        AgentOperation::MarketCapabilities
        | AgentOperation::MarketMicrostructure { .. }
        | AgentOperation::FindInstruments { .. }
        | AgentOperation::MarketOverview { .. } => Err("RESEARCH_OWNER_REQUIRED".into()),
        AgentOperation::Trade { request } => {
            ctx.finance
                .trade()?
                .trade_with_invocation(
                    account.unwrap_or_default(),
                    request,
                    g,
                    Some(crate::trade_service::TradeInvocation {
                        request_id: &r.request_id,
                        origin: if matches!(
                            request,
                            okx_ipc_contracts::trading::TradeRequest::Status { .. }
                        ) {
                            "agent_request.trade.status"
                        } else {
                            "agent_request.trade"
                        },
                    }),
                )
                .await
        }
        AgentOperation::Strategies { request } => {
            ctx.finance
                .trade()?
                .strategies(account.unwrap_or_default(), request, g)
                .await
        }
        AgentOperation::Portfolio { request } => {
            crate::portfolio::portfolio(ctx.finance.accounts()?, request, ctx.finance.trade()?)
                .await
        }
        AgentOperation::Diagnostics { request }
            if request.view == Some(DiagnosticsView::Research) =>
        {
            Err("RESEARCH_OWNER_REQUIRED".into())
        }
        AgentOperation::Diagnostics { request } => {
            crate::diagnostics::diagnostics(ctx, request, account).await
        }
        AgentOperation::AccountSummary => {
            let a = ctx
                .finance
                .accounts()?
                .resolve(account.unwrap_or_default())
                .await
                .ok_or("ACCOUNT_UNAVAILABLE")?;
            let snapshot = a
                .latest_snapshot
                .read()
                .await
                .clone()
                .ok_or("WAITING_ACCOUNT_EVIDENCE")?;
            let observed = jiff::Timestamp::from_str(&snapshot.source_received_at)
                .map_err(|_| "ACCOUNT_SOURCE_TIME_INVALID")?
                .as_millisecond();
            let now = Timestamp::now().as_millisecond();
            if !crate::config::evidence_fresh(observed, now, crate::config::ACCOUNT_MAX_AGE_MS) {
                return Err("WAITING_ACCOUNT_EVIDENCE".into());
            }
            Ok(
                json!({"account":a.configuration.account_key.wire(),"source_received_at":snapshot.source_received_at,"total_equity_usd":snapshot.balance.total_equity_usd,"initial_margin_usd":snapshot.balance.initial_margin_requirement_usd,"notional_usd":snapshot.balance.notional_usd,"position_count":snapshot.positions.len(),"pending_order_count":snapshot.pending_orders.len(),"currencies":snapshot.balance.details.iter().take(16).map(|d|json!({"currency":d.currency,"equity":d.equity,"available":d.available_equity})).collect::<Vec<_>>() }),
            )
        }
        AgentOperation::MailboxTelemetry => Ok(
            json!({"contract":"okx.product/v1","installation_id":ctx.finance.accounts()?.installation_id()}),
        ),
        // In `cargo check --all-targets`, Cargo can unify the test-only IPC
        // feature into an ordinary library build even though `cfg(test)` is
        // false for that target. Reject only the retired Research variants;
        // this arm MUST remain last, after Trade/Strategies/Diagnostics.
        #[allow(unreachable_patterns)]
        _ => Err("RESEARCH_OWNER_REQUIRED".into()),
    }
}

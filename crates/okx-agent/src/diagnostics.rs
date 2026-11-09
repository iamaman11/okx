//! Read-only diagnostics project actual owners; they never change access or replay an intent.
mod protection_trace;
pub(crate) use protection_trace::{
    ProtectionCleanupInvocation, ProtectionOperationTrace, ProtectionTraceEvent,
};

use crate::{accounts::AccountResources, query::DispatchContext};
use chrono::Utc;
use futures_util::{StreamExt, stream};
use okx_execution::{AccountStore, StoreResult, StoredIntent};
use okx_protocol::{
    analytics::ResearchRequest,
    trading::{DiagnosticsRequest, DiagnosticsView},
};
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

async fn operation_diagnostics(
    ctx: &DispatchContext,
    request: &DiagnosticsRequest,
    route: Option<&str>,
) -> Result<Value, String> {
    if request.limit.is_some() || request.cycle_id.is_some() {
        return Err("INVALID_DIAGNOSTICS_VIEW".into());
    }
    let reference = request
        .reference
        .as_deref()
        .ok_or("OPERATION_REFERENCE_REQUIRED")?;
    let requested = request
        .account
        .as_deref()
        .or(route)
        .ok_or("ACCOUNT_REQUIRED")?;
    let account = ctx
        .accounts
        .resolve(requested)
        .await
        .ok_or("ACCOUNT_UNAVAILABLE")?;
    if let Some(route) = route {
        let selected = ctx
            .accounts
            .resolve(route)
            .await
            .ok_or("ACCOUNT_UNAVAILABLE")?;
        if selected.configuration.account_key != account.configuration.account_key {
            return Err("ACCOUNT_ROUTE_MISMATCH".into());
        }
    }
    match account.protection_operation_trace.read(reference) {
        Ok(state) if state["state"] == "NOT_OBSERVED" => Ok(json!({
            "schema":"okx.operation-diagnostics/v1",
            "account":account.configuration.account_key.wire(),
            "reference":reference,"state":"NOT_OBSERVED",
            "reason_code":"VOLATILE_PROJECTION_UNAVAILABLE",
            "quality":"partial",
            "send_and_ack_history":"UNKNOWN_AFTER_RESTART_OR_EVICTION",
            "trace_health":state["trace_health"],
            "next_action":"trade.status","read_only":true
        })),
        Ok(state) => Ok(json!({
            "schema":"okx.operation-diagnostics/v1",
            "account":account.configuration.account_key.wire(),
            "reference":reference,"state":"OBSERVED","operation":state,
            "quality":"partial",
            "read_only":true,"central_budget":{"state":"NOT_EVALUATED","reason_code":"DIAGNOSTICS_ONLY"}
        })),
        Err(code) => Ok(json!({
            "schema":"okx.operation-diagnostics/v1",
            "account":account.configuration.account_key.wire(),
            "reference":reference,"state":"NOT_EVALUATED",
            "reason_code":code,"next_action":"retry_read_only_diagnostic","read_only":true,
            "trace_health":account.protection_operation_trace.health_report(),
            "quality":"partial"
        })),
    }
}

fn report(
    stage: &str,
    reason: &str,
    scope: &str,
    blockers: Vec<String>,
    retryable: bool,
    next: &str,
) -> Value {
    json!({"stage":stage,"reason_code":reason,"scope":scope,"blockers":blockers,"retryable":retryable,"next_action":next,"as_of_ms":Utc::now().timestamp_millis(),"quality":if blockers.is_empty(){"complete"}else{"partial"}})
}
fn safe_code(message: &str, fallback: &str) -> String {
    let code = message.split(':').next().unwrap_or("");
    if !code.is_empty()
        && code.len() <= 96
        && code
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
    {
        code.into()
    } else {
        fallback.into()
    }
}
fn account_snapshot_fresh_at(source_as_of: Option<i64>, observed_now: i64) -> bool {
    source_as_of.is_some_and(|t| observed_now >= t && observed_now - t <= 5000)
}

fn storage_failure_code(error: &okx_execution::StoreError) -> String {
    safe_code(&error.to_string(), "ACCOUNT_STORAGE_UNAVAILABLE")
}
fn lookup(store: &AccountStore, reference: &str) -> StoreResult<Option<StoredIntent>> {
    if let Some(i) = store.intent(reference)? {
        return Ok(Some(i));
    }
    if let Some(i) = store.by_client_order_id(reference)? {
        return Ok(Some(i));
    }
    store.by_exchange_order_id(reference)
}
fn central_budget_not_evaluated() -> Value {
    json!({
        "state":"NOT_EVALUATED",
        "reason_code":"NO_LOCAL_RISKBOOK_SNAPSHOT",
        "quality":"partial",
        "source":"none",
        "external_calls":false
    })
}
pub async fn diagnostics(
    ctx: &DispatchContext,
    request: &DiagnosticsRequest,
    account: Option<&str>,
) -> Result<Value, String> {
    if request.view == Some(DiagnosticsView::Storage) {
        return storage_diagnostics(ctx, request, account).await;
    }
    if request.view == Some(DiagnosticsView::Research) {
        if request.cycle_id.is_some() || request.limit.is_some() {
            return Err("INVALID_DIAGNOSTICS_VIEW".into());
        }
        let id = request
            .reference
            .as_deref()
            .ok_or("JOB_REFERENCE_REQUIRED")?;
        if id.len() != 64
            || !id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("INVALID_JOB_REFERENCE".into());
        }
        // Research jobs belong to the authenticated installation, not a
        // financial account. Explicit account routing is inapplicable here.
        if request.account.is_some() || account.is_some() {
            return Err("RESEARCH_DIAGNOSTICS_ACCOUNT_NOT_APPLICABLE".into());
        }
        return Ok(ctx.analytics.diagnostic_job(id));
    }
    if request.view == Some(DiagnosticsView::Operation) {
        return Ok(operation_diagnostics(ctx, request, account)
            .await
            .unwrap_or_else(|reason| {
                report(
                    "operation",
                    &safe_code(&reason, "DIAGNOSTICS_UNAVAILABLE"),
                    request.account.as_deref().or(account).unwrap_or("engine"),
                    vec![safe_code(&reason, "DIAGNOSTICS_UNAVAILABLE")],
                    false,
                    "supply an authorized account and operation reference",
                )
            }));
    }
    // Finance evidence must be readable without SQL-owner or central RiskBook.
    if request.view == Some(DiagnosticsView::Finance) {
        return Ok(finance_diagnostics(ctx, request, account)
            .await
            .unwrap_or_else(|reason| {
                report(
                    "finance",
                    &safe_code(&reason, "DIAGNOSTICS_UNAVAILABLE"),
                    request.account.as_deref().or(account).unwrap_or("engine"),
                    vec![safe_code(&reason, "DIAGNOSTICS_UNAVAILABLE")],
                    false,
                    "supply an authorized account and valid finance cycle",
                )
            }));
    }
    if request.cycle_id.is_some() || request.limit.is_some() {
        return Ok(report(
            "finance",
            "INVALID_DIAGNOSTICS_VIEW",
            "engine",
            vec!["INVALID_DIAGNOSTICS_VIEW".into()],
            false,
            "use view=finance",
        ));
    }
    // Diagnostics are read-only and bounded. In particular, do not send a
    // RiskCommandAction::Summary over WSS here: the Engine has no local cached
    // RiskBook projection, so central risk remains explicitly unevaluated.
    let mut out = match diagnostics_inner(ctx, request, account).await {
        Ok(value) => value,
        Err(code) => report(
            "routing",
            &safe_code(&code, "DIAGNOSTICS_UNAVAILABLE"),
            request.account.as_deref().or(account).unwrap_or("engine"),
            vec![safe_code(&code, "DIAGNOSTICS_UNAVAILABLE")],
            false,
            "supply an explicit valid account and reference",
        ),
    };
    out["central_budget"] = central_budget_not_evaluated();
    Ok(out)
}
/// Explicit snapshot semantics preserve the existing default detailed SQL view.
/// The snapshot is never used as authority for an operation or account access.
async fn storage_diagnostics(
    ctx: &DispatchContext,
    request: &DiagnosticsRequest,
    route: Option<&str>,
) -> Result<Value, String> {
    if request.reference.is_some() || request.cycle_id.is_some() || request.limit.is_some() {
        return Err("INVALID_DIAGNOSTICS_VIEW".into());
    }
    let requested = request
        .account
        .as_deref()
        .or(route)
        .ok_or("ACCOUNT_REQUIRED")?;
    // Registry locks only: no credential lookup, SQL job or source refresh.
    let resources =
        tokio::time::timeout(Duration::from_millis(20), ctx.accounts.resolve(requested))
            .await
            .map_err(|_| "ACCOUNT_ROUTING_BUSY")?
            .ok_or("ACCOUNT_UNAVAILABLE")?;
    if let Some(route) = route {
        let selected = tokio::time::timeout(Duration::from_millis(20), ctx.accounts.resolve(route))
            .await
            .map_err(|_| "ACCOUNT_ROUTING_BUSY")?
            .ok_or("ACCOUNT_UNAVAILABLE")?;
        if selected.configuration.account_key != resources.configuration.account_key {
            return Err("ACCOUNT_ROUTE_MISMATCH".into());
        }
    }
    let observed = resources.execution.store.diagnostic_storage_snapshot();
    let now = Utc::now().timestamp_millis();
    let state = resources.private_ws.state();
    let private = state.try_read().ok().map(|s| {
        let status = s.status();
        json!({"connection_state":status.connection_state,"connection_generation":status.connection_generation,
            "logged_in":status.logged_in,"subscriptions_complete":status.subscriptions_complete,
            "last_inbound_ms":status.last_inbound_ms,"last_error_present":status.last_error.is_some()})
    });
    let source = resources.latest_snapshot.try_read().ok().map(|s| {
        let time = s
            .as_ref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s.source_received_at).ok())
            .map(|t| t.timestamp_millis());
        json!({"account_as_of_ms":time,"account_fresh":account_snapshot_fresh_at(time,now)})
    });
    let health = resources
        .source_capture_health
        .try_read()
        .ok()
        .map(|h| json!(&*h));
    let age = observed
        .snapshot
        .as_ref()
        .map(|s| now.saturating_sub(s.observed_at_ms).max(0));
    Ok(
        json!({"schema":"okx.storage-diagnostics/v1","owner":"account_sql","account":resources.configuration.account_key.wire(),
        "state":observed.state,"quality":"partial","read_only":true,"history":"UNKNOWN",
        "storage":observed,"snapshot_age_ms":age,"snapshot_semantics":"last_owner_publication_not_current_sql_read",
        "source_identity_verified":resources.diagnostic_source_identity(),
        "private_ws":private.unwrap_or_else(||json!({"state":"NOT_EVALUATED","reason_code":"PRIVATE_STATE_BUSY"})),
        "sources":source.unwrap_or_else(||json!({"state":"NOT_EVALUATED","reason_code":"SOURCE_SNAPSHOT_BUSY"})),
        "source_capture":health.unwrap_or_else(||json!({"state":"NOT_EVALUATED","reason_code":"HEALTH_BUSY"})),
        "access":ctx.trade.diagnostic_access_now(&resources.configuration.account_key.wire()),
        "central_budget":central_budget_not_evaluated(),"next_action":"inspect_snapshot_age_and_refresh_pending"}),
    )
}

async fn finance_diagnostics(
    ctx: &DispatchContext,
    request: &DiagnosticsRequest,
    route: Option<&str>,
) -> Result<Value, String> {
    if request.reference.is_some() {
        return Err("INVALID_DIAGNOSTICS_VIEW".into());
    }
    let limit = request.limit.unwrap_or(3);
    if !(1..=4).contains(&limit) {
        return Err("INVALID_DIAGNOSTICS_LIMIT".into());
    }
    let requested = request
        .account
        .as_deref()
        .or(route)
        .ok_or("ACCOUNT_REQUIRED")?;
    let resources = ctx
        .accounts
        .resolve(requested)
        .await
        .ok_or("ACCOUNT_UNAVAILABLE")?;
    if let Some(route) = route {
        let routed = ctx
            .accounts
            .resolve(route)
            .await
            .ok_or("ACCOUNT_UNAVAILABLE")?;
        if resources.configuration.account_key != routed.configuration.account_key {
            return Err("ACCOUNT_ROUTE_MISMATCH".into());
        }
    }
    let trace = resources
        .finance_trace
        .readback(request.cycle_id, usize::from(limit));
    let state = if trace.state == "NOT_EVALUATED" {
        "NOT_EVALUATED"
    } else if trace.selected_not_found || (trace.active.is_none() && trace.recent.is_empty()) {
        "NOT_PROVEN"
    } else {
        "OBSERVED"
    };
    // Never queue an explain behind a capture writer's health lock.
    // A busy health snapshot is NOT_EVALUATED, not a clean acceptance window.
    let window = resources
        .source_capture_health
        .try_read()
        .ok()
        .map(|health| health.finance_window.summary(Utc::now().timestamp_millis()));
    let mut maintenance = resources.execution.store.maintenance_timing();
    let now_ms = Utc::now().timestamp_millis();
    let active_cycle = trace.selected.as_ref().or(trace.active.as_ref());
    let overlap_ids: Vec<_> = active_cycle
        .map(|cycle| {
            let end = cycle.finished_ms.unwrap_or(now_ms);
            maintenance
                .events
                .iter()
                .filter(|event| {
                    event.started_ms <= end
                        && event.finished_ms.unwrap_or(now_ms) >= cycle.started_ms
                })
                .take(8)
                .map(|event| event.id)
                .collect()
        })
        .unwrap_or_default();
    // Correlate across full bounded 10-minute history, then serialize only four events.
    maintenance.events.truncate(4);
    let partial = trace.state != "OBSERVED"
        || trace.selected_not_found
        || trace.selected_details_unavailable
        || window.is_none()
        || maintenance.state != "OBSERVED";
    Ok(json!({
        "schema": "okx.finance-diagnostics/v1",
        "quality": if partial { "partial" } else { "complete" },
        "account": resources.configuration.account_key.wire(),
        "state": state,
        "trace": trace,
        "acceptance_window": match window {
            Some(window) => json!({
                "state": window.state,
                "samples": window.samples,
                "successes": window.successes,
                "failures": window.failures,
                "source_age_violations": window.source_age_violations,
                "tick_gaps": window.tick_gaps,
                "source_generation_changes": window.source_generation_changes,
                "capture_attempt_latency": window.capture_attempt_latency,
                "successful_nav_sql_owner_latency": window.successful_nav_sql_owner_latency,
                "successful_nav_sql_queue_latency": window.successful_nav_sql_queue_latency,
                "backup_evidence": window.backup_evidence,
            }),
            None => json!({"state":"NOT_EVALUATED","reason_code":"HEALTH_BUSY"}),
        },
        "operational_findings": {
            "sql_maintenance": maintenance,
            "maintenance_overlap_ids": overlap_ids,
        },
        "current_readiness": {"state":"NOT_EVALUATED","reason_code":"DIAGNOSTICS_ONLY"},
        "central_budget": {"state":"NOT_EVALUATED","reason_code":"DIAGNOSTICS_ONLY"},
        "next_action": if trace.state == "NOT_EVALUATED" {
            "retry read-only diagnostic; finance trace is busy"
        } else if trace.selected_not_found {
            "cycle not observed; prior process or evicted history remains unknown"
        } else if trace.selected_details_unavailable {
            "inspect retained cycle summary; detailed phases are unavailable"
        } else {
            "inspect measured cycle and failing phase"
        }
    }))
}

async fn diagnostics_inner(
    ctx: &DispatchContext,
    request: &DiagnosticsRequest,
    account: Option<&str>,
) -> Result<Value, String> {
    let explicit = request.account.as_deref().or(account);
    if let (Some(body), Some(route)) = (request.account.as_deref(), account) {
        let a = ctx
            .accounts
            .resolve(body)
            .await
            .ok_or("ACCOUNT_UNAVAILABLE")?;
        let b = ctx
            .accounts
            .resolve(route)
            .await
            .ok_or("ACCOUNT_UNAVAILABLE")?;
        if a.configuration.account_key != b.configuration.account_key {
            return Err("ACCOUNT_ROUTE_MISMATCH".into());
        }
    }
    if let Some(reference) = &request.reference
        && reference.len() == 64
        && reference.bytes().all(|b| b.is_ascii_hexdigit())
        && let Ok(job) = ctx
            .analytics
            .research(ResearchRequest::Status {
                job_id: reference.clone(),
            })
            .await
    {
        let state = job["state"].as_str().unwrap_or("unknown");
        let failure = job["error"].as_str();
        let (reason, retry, next) = match state {
            "completed" => ("JOB_COMPLETED", false, "read the bounded result page"),
            "queued" | "running" => (
                "JOB_RUNNING",
                true,
                "poll research.status or cancel the job",
            ),
            "cancelled" => (
                "JOB_CANCELLED",
                false,
                "submit a new request only if desired",
            ),
            "interrupted" => (
                "JOB_INTERRUPTED",
                false,
                "submit a new request with a new idempotency key",
            ),
            _ => (
                "RESEARCH_FAILED",
                false,
                "correct the reported source gap or parameters",
            ),
        };
        let reason = failure
            .map(|e| safe_code(e, reason))
            .unwrap_or(reason.into());
        let mut out = report(
            "research",
            &reason,
            &format!("job:{reference}"),
            if matches!(state, "failed" | "interrupted") {
                vec![reason.clone()]
            } else {
                vec![]
            },
            retry,
            next,
        );
        out["job"] = json!({"job_id":job["id"],"state":job["state"],"progress":job["progress"],"artifact":job["artifact"],"created_ms":job["created_ms"],"result_pinned":job["pinned"]});
        return Ok(out);
    }
    if let Some(key) = explicit {
        let Some(resources) = ctx.accounts.resolve(key).await else {
            return Ok(report(
                "account",
                "ACCOUNT_UNAVAILABLE",
                key,
                vec!["ACCOUNT_UNAVAILABLE".into()],
                true,
                "the Engine retries unavailable account sources; inspect account status",
            ));
        };
        return account_diagnostics(ctx, resources, request.reference.clone()).await;
    }
    let configurations = ctx.accounts.list().await;
    let total = configurations.len();
    if let Some(reference) = &request.reference {
        if total > 8 {
            return Ok(report(
                "routing",
                "ACCOUNT_REQUIRED_FOR_BOUNDED_SEARCH",
                "engine",
                vec!["MORE_THAN_EIGHT_ACCOUNTS".into()],
                false,
                "supply an explicit account alias or key",
            ));
        }
        let mut found = Vec::new();
        let mut incomplete = Vec::new();
        let reference = reference.clone();
        let matches = stream::iter(configurations.into_iter().map(|config| {
            let reference = reference.clone();
            async move {
                let key = config.account_key.wire();
                let Some(r) = ctx.accounts.resolve(&key).await else {
                    return (key, None, Some(String::from("ACCOUNT_UNAVAILABLE")));
                };
                let query = reference.clone();
                match r
                    .execution
                    .store
                    .call(Instant::now() + Duration::from_secs(3), move |s| {
                        lookup(s, &query)
                    })
                    .await
                {
                    Ok(Some(_)) => (key, Some(r), None),
                    Ok(None) => (key, None, None),
                    Err(_) => (key, None, Some(String::from("ACCOUNT_STORAGE_UNAVAILABLE"))),
                }
            }
        }))
        .buffer_unordered(4)
        .collect::<Vec<_>>()
        .await;
        for (key, resource, error) in matches {
            if let Some(r) = resource {
                found.push(r)
            }
            if let Some(code) = error {
                incomplete.push(json!({"account":key,"reason_code":code}));
            }
        }
        if found.len() > 1 {
            return Ok(report(
                "routing",
                "REFERENCE_AMBIGUOUS",
                "engine",
                vec!["REFERENCE_MATCHES_MULTIPLE_ACCOUNTS".into()],
                false,
                "supply the account explicitly; no selected-account fallback is used",
            ));
        }
        if !incomplete.is_empty() {
            let mut out = report(
                "routing",
                "REFERENCE_SEARCH_INCOMPLETE",
                "engine",
                vec!["ACCOUNT_SEARCH_UNAVAILABLE".into()],
                true,
                "supply an explicit account or wait for account storage recovery",
            );
            out["unavailable_accounts"] = json!(incomplete);
            return Ok(out);
        }
        return match found.pop() {
            Some(resource) => account_diagnostics(ctx, resource, Some(reference)).await,
            None => Ok(report(
                "routing",
                "REFERENCE_NOT_FOUND",
                "engine",
                vec!["REFERENCE_NOT_FOUND".into()],
                false,
                "check the reference and supply an account when appropriate",
            )),
        };
    }
    let mut rows = Vec::new();
    for c in configurations.into_iter().take(8) {
        let key = c.account_key.wire();
        let ready = match ctx.accounts.resolve(&key).await {
            Some(a) => Some(a.source_identity_ready().await),
            None => None,
        };
        rows.push(json!({"account":key,"alias":c.alias,"source_identity_ready":ready}));
    }
    let failures = ctx.accounts.failures().await;
    let mut out = report(
        "engine",
        "OBSERVED",
        "engine",
        if failures.is_empty() {
            vec![]
        } else {
            vec!["ACCOUNTS_UNAVAILABLE".into()]
        },
        !failures.is_empty(),
        "use an account, intent/client/exchange reference, or research job ID for details",
    );
    out["accounts"] = json!(rows);
    out["returned_accounts"] = json!(rows.len());
    out["total_accounts"] = json!(total);
    out["omitted_accounts"] = json!(total.saturating_sub(rows.len()));
    out["unavailable_accounts"] = json!(
        failures
            .keys()
            .take(8)
            .map(|key| json!({"account":key,"reason_code":"ACCOUNT_UNAVAILABLE"}))
            .collect::<Vec<_>>()
    );
    Ok(out)
}
async fn account_diagnostics(
    ctx: &DispatchContext,
    resources: Arc<AccountResources>,
    reference: Option<String>,
) -> Result<Value, String> {
    let key = resources.configuration.account_key.wire();
    let lookup_reference = reference.clone();
    let facts=resources.execution.store.call(Instant::now()+Duration::from_secs(3),move|s|{
  let intent=lookup_reference.as_deref().map(|r|lookup(s,r)).transpose()?.flatten();let policy=s.policy()?;let stops=s.stops()?;let(version,source_id)=s.sqlite_runtime()?;let(db,wal)=s.storage_bytes()?;let mut coverage=Vec::new();for(source,partition)in[("account_snapshot","account"),("fills","SPOT"),("fills","SWAP"),("fills","FUTURES"),("bills","account")]{let c=s.cursor(source,partition)?;coverage.push(json!({"source":source,"partition":partition,"state":if c.is_some(){"observed"}else{"not_evaluated"},"watermark":c.as_ref().map(|c|c.watermark),"generation":c.as_ref().map(|c|c.generation),"gaps":c.as_ref().map(|c|c.coverage_gaps.chars().take(256).collect::<String>())}));}
  Ok((intent,json!({"policy":{"revision":policy.0,"explicit_pause":policy.1,"campaign_id":policy.2,"campaign_limit_usd":policy.3.to_string(),"campaign_loss_usd":policy.4.to_string()},"stop_count":stops.len(),"stops":stops.iter().take(10).map(|(id,scope,reason,time)|json!({"id":id,"scope":scope,"reason_code":safe_code(reason,"ACCOUNT_STOP"),"latched_at_ms":time})).collect::<Vec<_>>(),"sqlite_version":version,"sqlite_source_id":source_id,"database_bytes":db,"wal_bytes":wal,"maintenance_error":s.storage_health().map(|e|safe_code(e,"STORAGE_MAINTENANCE_ERROR")),"source_coverage":coverage})))
 }).await;
    let (intent, facts) = match facts {
        Ok(f) => f,
        Err(error) => {
            let reason = storage_failure_code(&error);
            let out = report(
                "storage",
                &reason,
                &key,
                vec![reason.clone()],
                true,
                "the Engine retries safe storage reads; do not reinitialize an existing account",
            );
            return Ok(out);
        }
    };
    // A successful capture may publish a new snapshot while this diagnostic
    // awaits the SQL owner. Read the current source AND identity after that
    // await, otherwise a stale pre-wait clone can falsely block readiness.
    // Keep the 5000ms fail-closed gate for genuinely expired evidence.
    let identity_ready = resources.source_identity_ready().await;
    let source_as_of = resources
        .latest_snapshot
        .read()
        .await
        .as_ref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s.source_received_at).ok())
        .map(|d| d.timestamp_millis());
    let fresh = account_snapshot_fresh_at(source_as_of, Utc::now().timestamp_millis());
    let mut blockers = Vec::new();
    if !identity_ready || !fresh {
        blockers.push("WAITING_ACCOUNT_EVIDENCE".into());
    }
    if facts["policy"]["explicit_pause"] == true {
        blockers.push("USER_PAUSED".into());
    }
    if facts["stop_count"].as_u64().unwrap_or(0) > 0 {
        blockers.push("ACCOUNT_STOP".into());
    }
    if !facts["maintenance_error"].is_null() {
        blockers.push("STORAGE_MAINTENANCE_ERROR".into());
    }
    let (stage, reason, retry, next) = if let Some(i) = &intent {
        match i.state {
            okx_execution::IntentState::Posting | okx_execution::IntentState::Unknown => (
                "reconciliation",
                "PENDING_EXCHANGE_READBACK",
                true,
                "the Engine reconciles this client order ID; never resubmit an unknown outcome",
            ),
            _ if i.incident.is_some() => (
                "protection",
                "INTENT_INCIDENT",
                true,
                "the Engine maintains or reconciles protection; new risk remains constrained",
            ),
            _ if i.state.terminal() => (
                "intent",
                "TERMINAL_OUTCOME",
                false,
                "inspect the final intent state and attributed financial history",
            ),
            _ => (
                "intent",
                "INTENT_OBSERVED",
                true,
                "poll the same intent reference; retain its idempotency key",
            ),
        }
    } else if reference.is_some() {
        (
            "routing",
            "REFERENCE_NOT_FOUND",
            false,
            "verify the reference for this explicit account",
        )
    } else if !blockers.is_empty() {
        (
            "account",
            "ACCOUNT_READINESS_BLOCKERS",
            true,
            "sources recover automatically; an explicit user pause requires one resume command",
        )
    } else {
        (
            "account",
            "OBSERVED",
            false,
            "no diagnostic action required",
        )
    };
    if matches!(
        reason,
        "PENDING_EXCHANGE_READBACK" | "INTENT_INCIDENT" | "REFERENCE_NOT_FOUND"
    ) {
        blockers.push(reason.into());
    }
    let mut out = report(stage, reason, &key, blockers, retry, next);
    out["account"] = json!({"key":key,"alias":resources.configuration.alias});
    out["sources"] = json!({"identity_generation_verified":identity_ready,"account_as_of_ms":source_as_of,"account_fresh":fresh,"default_ttl_ms":5000});
    let private = resources.private_ws.status().await;
    out["private_ws"] = json!({
        "connection_state": private.connection_state,
        "connection_generation": private.connection_generation,
        "logged_in": private.logged_in,
        "subscriptions_complete": private.subscriptions_complete,
        "last_inbound_ms": private.last_inbound_ms,
        "last_error_present": private.last_error.is_some(),
    });
    // Bounded per-attempt observations share the existing account health lock.
    // Compute P95 on demand, never on the SQL writer or source capture path.
    let capture_health = resources.source_capture_health.read().await;
    out["finance_window"] = json!(
        capture_health
            .finance_window
            .summary(Utc::now().timestamp_millis())
    );
    out["source_capture"] = json!(&*capture_health);
    drop(capture_health);
    out["storage"] = facts;
    // Existing diagnostics surface; no new transport, DB owner or persistent journal.
    out["storage"]["account_sql_timing"] = json!(resources.execution.store.sql_timing());
    out["access"] = ctx.trade.diagnostic_access(&key).await;
    out["intent"]=intent.as_ref().map(|i|json!({"intent_id":i.intent_id,"client_order_id":i.client_order_id,"exchange_order_id":i.exchange_order_id,"state":i.state.as_str(),"instrument":i.instrument_id,"risk_increase":i.risk_increase,"policy_revision":i.policy_revision,"revision":i.revision,"deadline_ms":i.deadline,"incident":i.incident.as_ref().map(|e|safe_code(e,"INTENT_INCIDENT"))})).unwrap_or(Value::Null);
    Ok(out)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn default_diagnostics_never_claims_a_remote_risk_summary() {
        let budget = central_budget_not_evaluated();
        assert_eq!(budget["state"], "NOT_EVALUATED");
        assert_eq!(budget["reason_code"], "NO_LOCAL_RISKBOOK_SNAPSHOT");
        assert_eq!(budget["source"], "none");
        assert_eq!(budget["external_calls"], false);
        let response = crate::query::response(
            "local-diagnostic",
            Ok(json!({"quality":"complete", "central_budget":budget})),
        );
        assert_eq!(response.quality, okx_protocol::DataQuality::Degraded);
    }

    #[test]
    fn operation_projection_reads_are_local_bounded_and_isolated() {
        let a = ProtectionOperationTrace::default();
        let b = ProtectionOperationTrace::default();
        a.record(
            "cleanup_a",
            "intent-a",
            "client-a",
            ProtectionTraceEvent::Begin,
        );
        a.record(
            "cleanup_a",
            "intent-a",
            "client-a",
            ProtectionTraceEvent::AdapterInvoked,
        );
        a.record(
            "cleanup_a",
            "intent-a",
            "client-a",
            ProtectionTraceEvent::Unproven("PROTECTION_CANCEL_OUTCOME_UNKNOWN".into()),
        );
        let unproven = a.read("intent-a").unwrap();
        assert_eq!(unproven["snapshot"]["durable_state"], "POSTING");
        assert_eq!(
            unproven["snapshot"]["adapter_attempt"],
            "INVOKED_OUTBOUND_UNPROVEN"
        );
        assert_eq!(unproven["snapshot"]["venue_ack"], "NOT_OBSERVED");
        let absent = b.read("intent-a").unwrap();
        assert_eq!(absent["state"], "NOT_OBSERVED");
        assert_eq!(absent["trace_health"]["history_complete"], "UNKNOWN");
        a.record(
            "cleanup_a",
            "intent-a",
            "client-a",
            ProtectionTraceEvent::Verified,
        );
        let verified = a.read("cleanup_a").unwrap();
        assert_eq!(verified["snapshot"]["durable_state"], "VERIFIED_CANCELED");
        assert!(verified["snapshot"]["missing_evidence"].is_null());
        let mut samples = Vec::new();
        for _ in 0..1_000 {
            let start = Instant::now();
            assert_eq!(a.read("intent-a").unwrap()["state"], "OBSERVED");
            samples.push(start.elapsed());
        }
        let raw_sample_ns = samples
            .iter()
            .map(|sample| sample.as_nanos())
            .collect::<Vec<_>>();
        samples.sort();
        println!(
            "PROTECTION_PROJECTION_BENCHMARK={}",
            json!({
                "scope": "local_projection_read_only_not_public_dispatch",
                "samples_ns": raw_sample_ns,
                "p95_ns": samples[949].as_nanos(),
                "build": if cfg!(debug_assertions) { "debug" } else { "release" },
                "arch": std::env::consts::ARCH,
                "os": std::env::consts::OS,
                "external_calls": 0,
                "mutations": 0
            })
        );
        assert!(
            samples[949] <= Duration::from_millis(100),
            "1,000 warm local reads must have P95 <=100ms"
        );
    }
    #[test]
    fn repeated_inspect_retains_locally_observed_ack_and_precise_missing_proof() {
        let trace = ProtectionOperationTrace::default();
        trace.record(
            "cleanup_a",
            "intent-a",
            "client-a",
            ProtectionTraceEvent::Begin,
        );
        trace.record(
            "cleanup_a",
            "intent-a",
            "client-a",
            ProtectionTraceEvent::AdapterInvoked,
        );
        trace.record(
            "cleanup_a",
            "intent-a",
            "client-a",
            ProtectionTraceEvent::Ack,
        );
        trace.record(
            "cleanup_a",
            "intent-a",
            "client-a",
            ProtectionTraceEvent::Unproven("PROTECTION_CANCEL_READBACK_STALE".into()),
        );
        let before = trace.read("cleanup_a").unwrap();
        assert_eq!(before["snapshot"]["venue_ack"], "OBSERVED");
        assert_eq!(
            before["snapshot"]["first_unproven_boundary"],
            "POST_ACK_READBACK"
        );
        trace.record(
            "cleanup_a",
            "intent-a",
            "client-a",
            ProtectionTraceEvent::Inspect,
        );
        let after = trace.read("cleanup_a").unwrap();
        assert_eq!(after["snapshot"]["venue_ack"], "OBSERVED");
        assert_eq!(
            after["snapshot"]["adapter_attempt"],
            "INVOKED_OUTBOUND_UNPROVEN"
        );
        assert_eq!(
            after["snapshot"]["first_unproven_boundary"],
            "POST_BEGIN_READBACK"
        );
        assert_eq!(
            after["snapshot"]["missing_evidence"],
            "SIGNED_EMPTY_ALGO_AND_FLAT_POSITION"
        );
        assert_eq!(after["trading_side_effects"], false);

        // Without this process' own ACK event, even a persisted BEGIN may
        // have reached the venue: never invent an ACK from the SQL receipt.
        let restarted = ProtectionOperationTrace::default();
        restarted.record(
            "cleanup_a",
            "intent-a",
            "client-a",
            ProtectionTraceEvent::Inspect,
        );
        let unknown = restarted.read("cleanup_a").unwrap();
        assert_eq!(unknown["snapshot"]["venue_ack"], "UNKNOWN_PRIOR_CALL");
        assert_eq!(unknown["snapshot"]["adapter_attempt"], "UNKNOWN_PRIOR_CALL");
    }

    #[test]
    fn diagnostics_operation_variant_roundtrip() {
        let v: DiagnosticsView = serde_json::from_str(r#""operation""#).unwrap();
        assert_eq!(v, DiagnosticsView::Operation);
        assert_eq!(serde_json::to_string(&v).unwrap(), r#""operation""#);
    }

    #[test]
    fn diagnostic_freshness_is_evaluated_at_response_not_before_sql_wait() {
        let sampled_at = 1_000_000_i64;
        assert!(account_snapshot_fresh_at(
            Some(sampled_at),
            sampled_at + 5_000
        ));
        assert!(!account_snapshot_fresh_at(
            Some(sampled_at),
            sampled_at + 5_001
        ));
        assert!(!account_snapshot_fresh_at(Some(sampled_at), sampled_at - 1));
        assert!(!account_snapshot_fresh_at(None, sampled_at));
        // A snapshot can be fresh when a SQL query is enqueued, but old
        // when the same request returns after the owner was blocked.
        assert!(account_snapshot_fresh_at(
            Some(sampled_at),
            sampled_at + 2_000
        ));
        assert!(!account_snapshot_fresh_at(
            Some(sampled_at),
            sampled_at + 7_000
        ));
    }

    #[tokio::test]
    async fn refreshed_snapshot_after_sql_wait_avoids_false_stale_without_weakening_ttl() {
        let published = tokio::sync::RwLock::new(Some(1_000_000_i64));
        let sampled_before_wait = *published.read().await;
        // Finance publishes a newer source while a diagnostic is queued.
        *published.write().await = Some(1_006_000);
        let sampled_after_wait = *published.read().await;
        assert!(!account_snapshot_fresh_at(sampled_before_wait, 1_007_000));
        assert!(account_snapshot_fresh_at(sampled_after_wait, 1_007_000));
        // A genuinely expired source must still fail closed.
        assert!(!account_snapshot_fresh_at(sampled_after_wait, 1_011_001));
    }

    #[test]
    fn distinguish_sql_queue_deadlines_from_real_storage_outages() {
        use okx_execution::StoreError;
        assert_eq!(
            storage_failure_code(&StoreError::Invalid("ACCOUNT_QUEUE_DEADLINE")),
            "ACCOUNT_QUEUE_DEADLINE"
        );
        assert_eq!(
            storage_failure_code(&StoreError::Invalid("ACCOUNT_STORAGE_DEADLINE")),
            "ACCOUNT_STORAGE_DEADLINE"
        );
        assert_eq!(
            storage_failure_code(&StoreError::Invalid("ACCOUNT_STORAGE_BACKPRESSURE")),
            "ACCOUNT_STORAGE_BACKPRESSURE"
        );
        assert_eq!(
            storage_failure_code(&StoreError::Io(std::io::Error::other("disk detail"))),
            "ACCOUNT_STORAGE_UNAVAILABLE"
        );
    }

    #[test]
    fn common_failure_fields_and_no_secret_debug_dump() {
        let r = report(
            "reconciliation",
            "PENDING_EXCHANGE_READBACK",
            "intent:fixture",
            vec!["POSSIBLE_EXPOSURE".into()],
            true,
            "poll the same intent",
        );
        assert_eq!(r["stage"], "reconciliation");
        assert_eq!(r["quality"], "partial");
        assert_eq!(r["retryable"], true);
        assert!(r["as_of_ms"].as_i64().unwrap() > 0);
        assert_eq!(
            safe_code(
                "api secret-key should never be included",
                "SOURCE_UNAVAILABLE"
            ),
            "SOURCE_UNAVAILABLE"
        );
        assert_eq!(
            safe_code("CANDLE_RANGE_INCOMPLETE: detail", "SOURCE_UNAVAILABLE"),
            "CANDLE_RANGE_INCOMPLETE"
        );
    }
}

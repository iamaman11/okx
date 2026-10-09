use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use serde::Serialize;

pub const RATE_THROTTLE_SCHEMA_V1: &str = "okx.rate-throttle/v1";
pub const RATE_BUDGET_SNAPSHOT_SCHEMA_V1: &str = "okx.rate-budget/v1";
pub const GENERAL_RATE_LIMIT_CODE: &str = "50011";
pub const SUBACCOUNT_RATE_LIMIT_CODE: &str = "50061";
pub const DEFAULT_SUBACCOUNT_ORDER_LIMIT_PER_2S: u32 = 1_000;

const LOCAL_FALLBACK_LIMIT: u32 = 1;
const LOCAL_FALLBACK_WINDOW_MS: u64 = 1_000;
const TRADE_INSTRUMENT_LIMIT_PER_2S: u32 = 60;
const TRADE_WINDOW_MS: u64 = 2_000;
const WS_CONTROL_LIMIT_PER_HOUR: u32 = 480;
const WS_CONTROL_WINDOW_MS: u64 = 60 * 60 * 1_000;
const MAX_LOCAL_DEFER_MS: u64 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RateOperationClass {
    PublicRestRead,
    PrivateRestRead,
    WsLogin,
    WsSubscribe,
    WsUnsubscribe,
    PlaceOrder,
    AmendOrder,
    CancelOrder,
    WsPlaceOrder,
    WsAmendOrder,
    WsCancelOrder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RateDomainKind {
    PublicRestIp,
    PrivateRestUser,
    WsConnectionControl,
    WsOrderUser,
    TradePlaceInstrument,
    TradeAmendInstrument,
    TradeCancelInstrument,
    TradePlaceFamily,
    TradeAmendFamily,
    TradeCancelFamily,
    SubaccountAggregate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RateThrottleSource {
    LocalBudget,
    Exchange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RateDecision {
    Deferred,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RateDomainKey {
    kind: RateDomainKind,
    endpoint: Option<String>,
    scope: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RateWindowSpec {
    key: RateDomainKey,
    max_requests: u32,
    window_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateRequestPlan {
    operation: RateOperationClass,
    domains: Vec<RateWindowSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RateDomainEvidence {
    pub kind: RateDomainKind,
    pub endpoint: Option<String>,
    pub scope: Option<String>,
    pub local_max_requests: u32,
    pub local_window_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RateThrottleEvidence {
    pub schema: &'static str,
    pub source: RateThrottleSource,
    pub exchange_code: Option<String>,
    pub operation: RateOperationClass,
    pub domain: Box<RateDomainEvidence>,
    pub attempt_count: u32,
    pub local_defer_ms: u64,
    pub server_retry_after_ms: Option<u64>,
    pub request_sent: bool,
    pub retryable: bool,
    pub decision: RateDecision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RateBudgetSnapshot {
    pub schema: &'static str,
    pub tracked_domains: usize,
    pub current_subaccount_limit_per_2s: u32,
    pub next_subaccount_limit_per_2s: Option<u32>,
    pub exchange_rate_limit_observed_at_ms: Option<u64>,
    pub last_throttle: Option<RateThrottleEvidence>,
}

#[derive(Debug, Default)]
struct RateWindowState {
    attempts: VecDeque<Instant>,
    blocked_until: Option<Instant>,
}

#[derive(Debug)]
struct RateBudgetState {
    windows: BTreeMap<RateDomainKey, RateWindowState>,
    current_subaccount_limit_per_2s: u32,
    next_subaccount_limit_per_2s: Option<u32>,
    exchange_rate_limit_observed_at_ms: Option<u64>,
    last_throttle: Option<RateThrottleEvidence>,
}

impl Default for RateBudgetState {
    fn default() -> Self {
        Self {
            windows: BTreeMap::new(),
            current_subaccount_limit_per_2s: DEFAULT_SUBACCOUNT_ORDER_LIMIT_PER_2S,
            next_subaccount_limit_per_2s: None,
            exchange_rate_limit_observed_at_ms: None,
            last_throttle: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RateBudget {
    inner: Arc<Mutex<RateBudgetState>>,
}

impl RateBudget {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn public_rest_plan(&self, path: &str, params: &[(&str, String)]) -> RateRequestPlan {
        let (max_requests, window_ms, scope) = public_rest_policy(path, params);
        RateRequestPlan {
            operation: RateOperationClass::PublicRestRead,
            domains: vec![RateWindowSpec {
                key: RateDomainKey {
                    kind: RateDomainKind::PublicRestIp,
                    endpoint: Some(path.to_owned()),
                    scope,
                },
                max_requests,
                window_ms,
            }],
        }
    }

    pub fn private_rest_plan(&self, path: &str, params: &[(&str, String)]) -> RateRequestPlan {
        let (max_requests, window_ms, scope) = private_rest_policy(path, params);
        RateRequestPlan {
            operation: RateOperationClass::PrivateRestRead,
            domains: vec![RateWindowSpec {
                key: RateDomainKey {
                    kind: RateDomainKind::PrivateRestUser,
                    endpoint: Some(path.to_owned()),
                    scope,
                },
                max_requests,
                window_ms,
            }],
        }
    }

    pub fn trade_rest_plan(
        &self,
        operation: RateOperationClass,
        instrument_id: &str,
        instrument_family: Option<&str>,
    ) -> RateRequestPlan {
        let trade_domain = match (operation, instrument_family) {
            (RateOperationClass::PlaceOrder, Some(_)) => RateDomainKind::TradePlaceFamily,
            (RateOperationClass::AmendOrder, Some(_)) => RateDomainKind::TradeAmendFamily,
            (RateOperationClass::CancelOrder, Some(_)) => RateDomainKind::TradeCancelFamily,
            (RateOperationClass::PlaceOrder, None) => RateDomainKind::TradePlaceInstrument,
            (RateOperationClass::AmendOrder, None) => RateDomainKind::TradeAmendInstrument,
            (RateOperationClass::CancelOrder, None) => RateDomainKind::TradeCancelInstrument,
            _ => RateDomainKind::WsOrderUser,
        };
        let trade_scope = instrument_family.unwrap_or(instrument_id).to_owned();
        let mut domains = vec![RateWindowSpec {
            key: RateDomainKey {
                kind: trade_domain,
                endpoint: Some(
                    match operation {
                        RateOperationClass::PlaceOrder => "/api/v5/trade/order",
                        RateOperationClass::AmendOrder => "/api/v5/trade/amend-order",
                        RateOperationClass::CancelOrder => "/api/v5/trade/cancel-order",
                        _ => "trade",
                    }
                    .to_owned(),
                ),
                scope: Some(trade_scope),
            },
            max_requests: TRADE_INSTRUMENT_LIMIT_PER_2S,
            window_ms: TRADE_WINDOW_MS,
        }];

        if matches!(
            operation,
            RateOperationClass::PlaceOrder | RateOperationClass::AmendOrder
        ) {
            let current_limit = self.lock_state().current_subaccount_limit_per_2s.max(1);
            domains.push(RateWindowSpec {
                key: RateDomainKey {
                    kind: RateDomainKind::SubaccountAggregate,
                    endpoint: None,
                    scope: Some("authenticated_subaccount".to_owned()),
                },
                max_requests: current_limit,
                window_ms: TRADE_WINDOW_MS,
            });
        }

        RateRequestPlan { operation, domains }
    }

    pub fn ws_control_plan(
        &self,
        operation: RateOperationClass,
        connection_scope: impl Into<String>,
    ) -> RateRequestPlan {
        debug_assert!(matches!(
            operation,
            RateOperationClass::WsLogin
                | RateOperationClass::WsSubscribe
                | RateOperationClass::WsUnsubscribe
        ));
        RateRequestPlan {
            operation,
            domains: vec![RateWindowSpec {
                key: RateDomainKey {
                    kind: RateDomainKind::WsConnectionControl,
                    endpoint: None,
                    scope: Some(connection_scope.into()),
                },
                max_requests: WS_CONTROL_LIMIT_PER_HOUR,
                window_ms: WS_CONTROL_WINDOW_MS,
            }],
        }
    }

    pub fn ws_order_plan(
        &self,
        operation: RateOperationClass,
        instrument_id: &str,
        instrument_family: Option<&str>,
    ) -> RateRequestPlan {
        let rest_equivalent = match operation {
            RateOperationClass::WsPlaceOrder => RateOperationClass::PlaceOrder,
            RateOperationClass::WsAmendOrder => RateOperationClass::AmendOrder,
            RateOperationClass::WsCancelOrder => RateOperationClass::CancelOrder,
            _ => operation,
        };
        let mut plan = self.trade_rest_plan(rest_equivalent, instrument_id, instrument_family);
        plan.operation = operation;
        plan
    }

    pub fn admit(&self, plan: &RateRequestPlan) -> Result<(), Box<RateThrottleEvidence>> {
        self.admit_at(plan, Instant::now())
    }

    pub fn record_exchange_throttle(
        &self,
        plan: &RateRequestPlan,
        exchange_code: &str,
        server_retry_after_ms: Option<u64>,
    ) -> RateThrottleEvidence {
        let now = Instant::now();
        let local_defer_ms = exchange_defer_ms(plan, exchange_code, server_retry_after_ms);
        let domain = select_exchange_domain(plan, exchange_code);
        let evidence = RateThrottleEvidence {
            schema: RATE_THROTTLE_SCHEMA_V1,
            source: RateThrottleSource::Exchange,
            exchange_code: Some(exchange_code.to_owned()),
            operation: plan.operation,
            domain: Box::new(domain_evidence(domain)),
            attempt_count: 1,
            local_defer_ms,
            server_retry_after_ms,
            request_sent: true,
            retryable: true,
            decision: RateDecision::Deferred,
        };

        let mut state = self.lock_state();
        let blocked_until = now + Duration::from_millis(local_defer_ms);
        for spec in &plan.domains {
            let window = state.windows.entry(spec.key.clone()).or_default();
            window.blocked_until = Some(
                window
                    .blocked_until
                    .map_or(blocked_until, |current| current.max(blocked_until)),
            );
        }
        state.last_throttle = Some(evidence.clone());
        evidence
    }

    pub fn update_subaccount_rate_limit(
        &self,
        current_per_2s: u32,
        next_per_2s: Option<u32>,
        observed_at_ms: u64,
    ) {
        let mut state = self.lock_state();
        state.current_subaccount_limit_per_2s = current_per_2s.max(1);
        state.next_subaccount_limit_per_2s = next_per_2s.filter(|value| *value > 0);
        state.exchange_rate_limit_observed_at_ms = Some(observed_at_ms);
    }

    pub fn snapshot(&self) -> RateBudgetSnapshot {
        let state = self.lock_state();
        RateBudgetSnapshot {
            schema: RATE_BUDGET_SNAPSHOT_SCHEMA_V1,
            tracked_domains: state.windows.len(),
            current_subaccount_limit_per_2s: state.current_subaccount_limit_per_2s,
            next_subaccount_limit_per_2s: state.next_subaccount_limit_per_2s,
            exchange_rate_limit_observed_at_ms: state.exchange_rate_limit_observed_at_ms,
            last_throttle: state.last_throttle.clone(),
        }
    }

    fn admit_at(
        &self,
        plan: &RateRequestPlan,
        now: Instant,
    ) -> Result<(), Box<RateThrottleEvidence>> {
        let mut state = self.lock_state();
        let mut constraining: Option<(&RateWindowSpec, u64)> = None;

        for spec in &plan.domains {
            let window = state.windows.entry(spec.key.clone()).or_default();
            prune(window, spec.window_ms, now);

            if let Some(blocked_until) = window.blocked_until {
                if blocked_until > now {
                    let wait_ms = duration_ms_ceil(blocked_until.duration_since(now));
                    if constraining.is_none_or(|(_, current)| wait_ms > current) {
                        constraining = Some((spec, wait_ms));
                    }
                    continue;
                }
                window.blocked_until = None;
            }

            if window.attempts.len() >= spec.max_requests as usize
                && let Some(oldest) = window.attempts.front().copied()
            {
                let release_at = oldest + Duration::from_millis(spec.window_ms);
                let wait_ms = if release_at > now {
                    duration_ms_ceil(release_at.duration_since(now))
                } else {
                    1
                };
                if constraining.is_none_or(|(_, current)| wait_ms > current) {
                    constraining = Some((spec, wait_ms));
                }
            }
        }

        if let Some((spec, wait_ms)) = constraining {
            let evidence = RateThrottleEvidence {
                schema: RATE_THROTTLE_SCHEMA_V1,
                source: RateThrottleSource::LocalBudget,
                exchange_code: None,
                operation: plan.operation,
                domain: Box::new(domain_evidence(spec)),
                attempt_count: 1,
                local_defer_ms: wait_ms.clamp(1, MAX_LOCAL_DEFER_MS),
                server_retry_after_ms: None,
                request_sent: false,
                retryable: true,
                decision: RateDecision::Deferred,
            };
            state.last_throttle = Some(evidence.clone());
            return Err(Box::new(evidence));
        }

        for spec in &plan.domains {
            state
                .windows
                .entry(spec.key.clone())
                .or_default()
                .attempts
                .push_back(now);
        }
        Ok(())
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, RateBudgetState> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn prune(window: &mut RateWindowState, window_ms: u64, now: Instant) {
    let horizon = Duration::from_millis(window_ms);
    while window
        .attempts
        .front()
        .is_some_and(|attempt| now.duration_since(*attempt) >= horizon)
    {
        window.attempts.pop_front();
    }
}

fn duration_ms_ceil(duration: Duration) -> u64 {
    let millis = duration.as_millis();
    u64::try_from(millis)
        .unwrap_or(u64::MAX)
        .saturating_add((!duration.subsec_nanos().is_multiple_of(1_000_000)) as u64)
}

fn domain_evidence(spec: &RateWindowSpec) -> RateDomainEvidence {
    RateDomainEvidence {
        kind: spec.key.kind,
        endpoint: spec.key.endpoint.clone(),
        scope: spec.key.scope.clone(),
        local_max_requests: spec.max_requests,
        local_window_ms: spec.window_ms,
    }
}

fn select_exchange_domain<'a>(
    plan: &'a RateRequestPlan,
    exchange_code: &str,
) -> &'a RateWindowSpec {
    if exchange_code == SUBACCOUNT_RATE_LIMIT_CODE
        && let Some(domain) = plan
            .domains
            .iter()
            .find(|domain| domain.key.kind == RateDomainKind::SubaccountAggregate)
    {
        return domain;
    }
    plan.domains
        .first()
        .expect("rate request plan always has at least one domain")
}

fn exchange_defer_ms(
    plan: &RateRequestPlan,
    exchange_code: &str,
    server_retry_after_ms: Option<u64>,
) -> u64 {
    let local = if exchange_code == SUBACCOUNT_RATE_LIMIT_CODE {
        TRADE_WINDOW_MS
    } else {
        plan.domains
            .iter()
            .map(|domain| domain.window_ms)
            .max()
            .unwrap_or(LOCAL_FALLBACK_WINDOW_MS)
            .clamp(250, TRADE_WINDOW_MS)
    };
    server_retry_after_ms
        .map_or(local, |server| server.max(local))
        .clamp(1, MAX_LOCAL_DEFER_MS)
}

fn public_rest_policy(path: &str, params: &[(&str, String)]) -> (u32, u64, Option<String>) {
    let (max_requests, window_ms) = match path {
        "/api/v5/public/time" => (5, 2_000),
        "/api/v5/public/instruments" => (10, 2_000),
        "/api/v5/market/ticker" => (10, 2_000),
        "/api/v5/market/tickers" => (20, 2_000),
        "/api/v5/market/trades" => (100, 2_000),
        "/api/v5/public/mark-price" => (10, 2_000),
        "/api/v5/market/index-tickers" => (10, 2_000),
        "/api/v5/public/funding-rate" => (10, 2_000),
        "/api/v5/public/funding-rate-history" => (10, 2_000),
        "/api/v5/public/open-interest" => (10, 2_000),
        "/api/v5/rubik/stat/contracts/open-interest-history" => (10, 2_000),
        "/api/v5/market/history-candles" => (10, 2_000),
        "/api/v5/public/price-limit" => (10, 2_000),
        "/api/v5/system/status" => (1, 1_000),
        _ => (LOCAL_FALLBACK_LIMIT, LOCAL_FALLBACK_WINDOW_MS),
    };
    let scope = match path {
        "/api/v5/public/funding-rate-history"
        | "/api/v5/rubik/stat/contracts/open-interest-history" => param(params, "instId")
            .map(|value| format!("public_ip+instrument:{value}"))
            .or_else(|| Some("public_ip".to_owned())),
        _ => Some("public_ip".to_owned()),
    };
    (max_requests, window_ms, scope)
}

fn private_rest_policy(path: &str, params: &[(&str, String)]) -> (u32, u64, Option<String>) {
    let scope = if path == "/api/v5/account/instruments" {
        param(params, "instType").map(|value| format!("authenticated_user+inst_type:{value}"))
    } else if path == "/api/v5/trade/order" {
        param(params, "instId").map(|value| format!("authenticated_user+instrument:{value}"))
    } else {
        Some("authenticated_user".to_owned())
    };
    let (max_requests, window_ms) = match path {
        "/api/v5/account/config" => (5, 2_000),
        "/api/v5/account/instruments" => (20, 2_000),
        "/api/v5/account/max-size" => (20, 2_000),
        "/api/v5/account/leverage-info" => (20, 2_000),
        "/api/v5/account/trade-fee" => (5, 2_000),
        "/api/v5/account/balance" => (10, 2_000),
        "/api/v5/account/positions" => (10, 2_000),
        "/api/v5/account/account-position-risk" => (10, 2_000),
        "/api/v5/account/position-builder" => (2, 2_000),
        "/api/v5/account/positions-history" => (10, 2_000),
        "/api/v5/asset/balances" => (6, 1_000),
        "/api/v5/trade/orders-pending" => (60, 2_000),
        "/api/v5/trade/orders-algo-pending" => (20, 2_000),
        "/api/v5/trade/order-algo" => (20, 2_000),
        "/api/v5/trade/orders-history" => (40, 2_000),
        "/api/v5/trade/orders-history-archive" => (20, 2_000),
        "/api/v5/trade/fills-history" => (10, 2_000),
        "/api/v5/account/bills-archive" => (5, 2_000),
        "/api/v5/trade/account-rate-limit" => (1, 1_000),
        "/api/v5/trade/order" => (60, 2_000),
        _ => (LOCAL_FALLBACK_LIMIT, LOCAL_FALLBACK_WINDOW_MS),
    };
    (max_requests, window_ms, scope)
}

fn param<'a>(params: &'a [(&str, String)], key: &str) -> Option<&'a str> {
    params
        .iter()
        .find_map(|(candidate, value)| (*candidate == key).then_some(value.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rest_endpoints_are_bounded_in_distinct_named_domains() {
        let budget = RateBudget::new();
        let config = budget.private_rest_plan("/api/v5/account/config", &[]);
        let positions = budget.private_rest_plan("/api/v5/account/positions", &[]);
        let position_risk = budget.private_rest_plan("/api/v5/account/account-position-risk", &[]);
        let position_builder = budget.private_rest_plan("/api/v5/account/position-builder", &[]);
        assert_ne!(config.domains[0].key, positions.domains[0].key);
        assert_eq!(position_risk.domains[0].max_requests, 10);
        assert_eq!(position_builder.domains[0].max_requests, 2);
        assert_eq!(position_builder.domains[0].window_ms, 2_000);
        assert_eq!(position_risk.domains[0].window_ms, 2_000);
        assert_eq!(config.domains[0].key.kind, RateDomainKind::PrivateRestUser);
        assert_eq!(positions.domains[0].max_requests, 10);
    }

    #[test]
    fn funding_balance_endpoint_uses_documented_user_budget() {
        let budget = RateBudget::new();
        let plan = budget.private_rest_plan("/api/v5/asset/balances", &[]);
        assert_eq!(plan.domains.len(), 1);
        assert_eq!(plan.domains[0].key.kind, RateDomainKind::PrivateRestUser);
        assert_eq!(
            plan.domains[0].key.scope.as_deref(),
            Some("authenticated_user")
        );
        assert_eq!(plan.domains[0].max_requests, 6);
        assert_eq!(plan.domains[0].window_ms, 1_000);
    }

    #[test]
    fn recent_order_history_uses_documented_user_budget_for_both_instrument_types() {
        let budget = RateBudget::new();
        let swap = budget.private_rest_plan(
            "/api/v5/trade/orders-history",
            &[("instType", "SWAP".to_owned())],
        );
        let futures = budget.private_rest_plan(
            "/api/v5/trade/orders-history",
            &[("instType", "FUTURES".to_owned())],
        );
        assert_eq!(swap.domains[0].max_requests, 40);
        assert_eq!(swap.domains[0].window_ms, 2_000);
        assert_eq!(swap.domains[0].key.kind, RateDomainKind::PrivateRestUser);
        assert_eq!(swap.domains[0].key, futures.domains[0].key);
        budget
            .admit(&swap)
            .expect("first recent order read admitted");
        budget
            .admit(&futures)
            .expect("second recent order read admitted");
    }

    #[test]
    fn protective_algo_reads_share_named_per_user_budget() {
        let budget = RateBudget::new();
        let swap = budget.private_rest_plan(
            "/api/v5/trade/orders-algo-pending",
            &[("instType", "SWAP".to_owned())],
        );
        let futures = budget.private_rest_plan(
            "/api/v5/trade/orders-algo-pending",
            &[("instType", "FUTURES".to_owned())],
        );
        assert_eq!(swap.domains[0].max_requests, 20);
        assert_eq!(swap.domains[0].window_ms, 2_000);
        assert_eq!(swap.domains[0].key, futures.domains[0].key);
        assert_eq!(
            budget
                .private_rest_plan("/api/v5/trade/order-algo", &[])
                .domains[0]
                .max_requests,
            20
        );
        budget
            .admit(&swap)
            .expect("SWAP pending algo read admitted");
        budget
            .admit(&futures)
            .expect("FUTURES pending algo read admitted");
    }

    #[test]
    fn documented_history_endpoint_budgets_are_not_exceeded() {
        let budget = RateBudget::new();
        let positions = budget.private_rest_plan("/api/v5/account/positions-history", &[]);
        let recent_orders = budget.private_rest_plan("/api/v5/trade/orders-history", &[]);
        let orders = budget.private_rest_plan("/api/v5/trade/orders-history-archive", &[]);
        let fills = budget.private_rest_plan("/api/v5/trade/fills-history", &[]);
        let bills = budget.private_rest_plan("/api/v5/account/bills-archive", &[]);
        assert_eq!(positions.domains[0].max_requests, 10);
        assert_eq!(recent_orders.domains[0].max_requests, 40);
        assert_eq!(recent_orders.domains[0].window_ms, 2_000);
        assert_eq!(orders.domains[0].max_requests, 20);
        assert_eq!(fills.domains[0].max_requests, 10);
        assert_eq!(bills.domains[0].max_requests, 5);
        assert!(
            positions
                .domains
                .iter()
                .all(|domain| domain.window_ms == 2_000)
        );
        assert!(
            orders
                .domains
                .iter()
                .all(|domain| domain.window_ms == 2_000)
        );
        assert!(fills.domains.iter().all(|domain| domain.window_ms == 2_000));
        assert!(bills.domains.iter().all(|domain| domain.window_ms == 2_000));
    }

    #[test]
    fn stage2_research_endpoints_use_documented_rate_domains() {
        let budget = RateBudget::new();
        let trades = budget.public_rest_plan(
            "/api/v5/market/trades",
            &[("instId", "BTC-USDT-SWAP".to_owned())],
        );
        assert_eq!(trades.domains[0].max_requests, 100);
        assert_eq!(trades.domains[0].window_ms, 2_000);
        assert_eq!(trades.domains[0].key.scope.as_deref(), Some("public_ip"));

        let funding_btc = budget.public_rest_plan(
            "/api/v5/public/funding-rate-history",
            &[("instId", "BTC-USDT-SWAP".to_owned())],
        );
        let funding_eth = budget.public_rest_plan(
            "/api/v5/public/funding-rate-history",
            &[("instId", "ETH-USDT-SWAP".to_owned())],
        );
        assert_eq!(funding_btc.domains[0].max_requests, 10);
        assert_eq!(funding_btc.domains[0].window_ms, 2_000);
        assert_ne!(funding_btc.domains[0].key, funding_eth.domains[0].key);

        let oi_btc = budget.public_rest_plan(
            "/api/v5/rubik/stat/contracts/open-interest-history",
            &[("instId", "BTC-USDT-SWAP".to_owned())],
        );
        let oi_eth = budget.public_rest_plan(
            "/api/v5/rubik/stat/contracts/open-interest-history",
            &[("instId", "ETH-USDT-SWAP".to_owned())],
        );
        assert_eq!(oi_btc.domains[0].max_requests, 10);
        assert_eq!(oi_btc.domains[0].window_ms, 2_000);
        assert_ne!(oi_btc.domains[0].key, oi_eth.domains[0].key);
    }

    #[test]
    fn distinct_instrument_oi_history_requests_do_not_share_one_request_fallback_budget() {
        let budget = RateBudget::new();
        let now = Instant::now();
        for instrument in [
            "BTC-USD_UM_XPERP-310404",
            "BTC-USD_UM-261030",
            "BTC-USD_UM-261127",
            "BTC-USD_UM-261225",
        ] {
            let plan = budget.public_rest_plan(
                "/api/v5/rubik/stat/contracts/open-interest-history",
                &[("instId", instrument.to_owned())],
            );
            budget
                .admit_at(&plan, now)
                .expect("documented IP+instrument budget admits distinct instruments");
        }
    }

    #[test]
    fn documented_bulk_tickers_budget_is_bounded() {
        let budget = RateBudget::new();
        let plan =
            budget.public_rest_plan("/api/v5/market/tickers", &[("instType", "SWAP".to_owned())]);
        assert_eq!(plan.domains[0].key.kind, RateDomainKind::PublicRestIp);
        assert_eq!(plan.domains[0].max_requests, 20);
        assert_eq!(plan.domains[0].window_ms, 2_000);
    }

    #[test]
    fn local_budget_returns_typed_defer_without_sending_request() {
        let budget = RateBudget::new();
        let plan = RateRequestPlan {
            operation: RateOperationClass::PrivateRestRead,
            domains: vec![RateWindowSpec {
                key: RateDomainKey {
                    kind: RateDomainKind::PrivateRestUser,
                    endpoint: Some("/test".to_owned()),
                    scope: Some("authenticated_user".to_owned()),
                },
                max_requests: 1,
                window_ms: 1_000,
            }],
        };
        let now = Instant::now();
        budget.admit_at(&plan, now).expect("first admitted");
        let evidence = budget
            .admit_at(&plan, now + Duration::from_millis(1))
            .expect_err("second deferred");
        assert_eq!(evidence.source, RateThrottleSource::LocalBudget);
        assert_eq!(evidence.decision, RateDecision::Deferred);
        assert!(!evidence.request_sent);
        assert!(evidence.retryable);
        assert_eq!(evidence.server_retry_after_ms, None);
        assert!(evidence.local_defer_ms > 0);
    }

    #[test]
    fn place_and_amend_share_parallel_subaccount_budget_but_cancel_does_not() {
        let budget = RateBudget::new();
        let place = budget.trade_rest_plan(RateOperationClass::PlaceOrder, "DOGE-USDT-SWAP", None);
        let amend = budget.trade_rest_plan(RateOperationClass::AmendOrder, "DOGE-USDT-SWAP", None);
        let cancel =
            budget.trade_rest_plan(RateOperationClass::CancelOrder, "DOGE-USDT-SWAP", None);
        assert!(
            place
                .domains
                .iter()
                .any(|domain| { domain.key.kind == RateDomainKind::SubaccountAggregate })
        );
        assert!(
            amend
                .domains
                .iter()
                .any(|domain| { domain.key.kind == RateDomainKind::SubaccountAggregate })
        );
        assert!(
            !cancel
                .domains
                .iter()
                .any(|domain| { domain.key.kind == RateDomainKind::SubaccountAggregate })
        );
    }

    #[test]
    fn exchange_50061_is_attributed_to_subaccount_domain_without_fake_retry_after() {
        let budget = RateBudget::new();
        let plan = budget.trade_rest_plan(RateOperationClass::PlaceOrder, "DOGE-USDT-SWAP", None);
        let evidence = budget.record_exchange_throttle(&plan, SUBACCOUNT_RATE_LIMIT_CODE, None);
        assert_eq!(evidence.source, RateThrottleSource::Exchange);
        assert_eq!(evidence.domain.kind, RateDomainKind::SubaccountAggregate);
        assert_eq!(evidence.exchange_code.as_deref(), Some("50061"));
        assert_eq!(evidence.server_retry_after_ms, None);
        assert_eq!(evidence.local_defer_ms, 2_000);
        assert!(evidence.request_sent);
    }

    #[test]
    fn account_rate_limit_evidence_updates_dynamic_subaccount_capacity() {
        let budget = RateBudget::new();
        budget.update_subaccount_rate_limit(2_000, Some(1_750), 1_790_000_000_000);
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.current_subaccount_limit_per_2s, 2_000);
        assert_eq!(snapshot.next_subaccount_limit_per_2s, Some(1_750));
        assert_eq!(
            snapshot.exchange_rate_limit_observed_at_ms,
            Some(1_790_000_000_000)
        );
        let plan = budget.trade_rest_plan(RateOperationClass::AmendOrder, "DOGE-USDT-SWAP", None);
        let aggregate = plan
            .domains
            .iter()
            .find(|domain| domain.key.kind == RateDomainKind::SubaccountAggregate)
            .expect("aggregate domain");
        assert_eq!(aggregate.max_requests, 2_000);
    }

    #[test]
    fn websocket_control_is_connection_scoped_and_bounded_to_documented_hourly_budget() {
        let budget = RateBudget::new();
        let plan = budget.ws_control_plan(RateOperationClass::WsSubscribe, "public-generation-7");
        assert_eq!(plan.domains.len(), 1);
        assert_eq!(
            plan.domains[0].key.kind,
            RateDomainKind::WsConnectionControl
        );
        assert_eq!(plan.domains[0].max_requests, 480);
        assert_eq!(plan.domains[0].window_ms, 3_600_000);
        assert_eq!(
            plan.domains[0].key.scope.as_deref(),
            Some("public-generation-7")
        );
    }
}

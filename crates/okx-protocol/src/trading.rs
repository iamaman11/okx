//! Strict public trading, strategy and portfolio commands. Native owners classify risk.
use crate::numeric::{DecimalInputError, positive_decimal};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum TradeRequest {
    Capabilities {},
    Preflight {},
    Preview {
        operation: TradeOperation,
        strategy_instance: String,
        instrument: String,
        side: PositionSide,
        sizing: Sizing,
        order: OrderConstraints,
        protection: Option<Protection>,
    },
    Execute {
        idempotency_key: String,
        operation: TradeOperation,
        strategy_instance: String,
        instrument: String,
        side: PositionSide,
        sizing: Sizing,
        order: OrderConstraints,
        protection: Option<Protection>,
        expires_in_ms: u64,
    },
    Status {
        reference: String,
    },
    Cancel {
        idempotency_key: String,
        reference: String,
    },
    Close {
        idempotency_key: String,
        reference: String,
        expires_in_ms: u64,
    },
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(untagged)]
pub enum Sizing {
    Contracts { contracts: String },
    MaxLossUsd { max_loss_usd: String },
    EntryBudgetUsd { entry_budget_usd: String },
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderConstraints {
    #[serde(rename = "type")]
    pub order_type: String,
    pub limit_price: Option<String>,
    pub reference: Option<String>,
    /// Optional independent fee-inclusive USD entry ceiling, NOT the modelled loss budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_entry_usd: Option<String>,
    pub max_slippage_bps: u32,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(untagged)]
pub enum Protection {
    Price { stop_price: String },
    Distance { stop_distance_bps: u32 },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeOperation {
    Open,
    Add,
    Reduce,
    Close,
    Replace,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionSide {
    Long,
    Short,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapitalDirection {
    Assign,
    Release,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum StrategyRequest {
    List {},
    CapitalAssign {
        idempotency_key: String,
        instance_id: String,
        direction: CapitalDirection,
        amount_usd: String,
        expected_authority_revision: u64,
    },
    Status {
        instance_id: String,
    },
    Create {
        idempotency_key: String,
        instance_id: String,
        definition_id: String,
        version: u32,
        parameters: serde_json::Value,
        instruments: Vec<String>,
        timeframe_ms: u64,
        budget_usd: String,
    },
    Update {
        idempotency_key: String,
        instance_id: String,
        expected_revision: u64,
        parameters: serde_json::Value,
        budget_usd: String,
    },
    Start {
        idempotency_key: String,
        instance_id: String,
    },
    Stop {
        idempotency_key: String,
        instance_id: String,
    },
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortfolioRequest {
    pub action: PortfolioAction,
    pub accounts: Vec<String>,
    pub start_ms: u64,
    pub end_ms: u64,
    pub strategies: Option<Vec<String>>,
    pub currencies: Option<Vec<String>>,
    pub cursor: Option<String>,
    pub limit: u16,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortfolioAction {
    Summary,
    History,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioPosition {
    pub instrument: String,
    pub side: PositionSide,
    pub notional_usd: String,
    pub margin_usd: String,
    pub price_shock_bps: i32,
    pub volatility_bps: u32,
    pub liquidity_cost_bps: u32,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioCorrelation {
    pub left: usize,
    pub right: usize,
    pub rho: f64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortfolioScenarioRequest {
    pub equity_usd: String,
    pub positions: Vec<ScenarioPosition>,
    pub correlations: Vec<ScenarioCorrelation>,
}
impl PortfolioScenarioRequest {
    pub fn valid(&self) -> bool {
        if !decimal(&self.equity_usd)
            || self.positions.is_empty()
            || self.positions.len() > 8
            || self.correlations.len() != self.positions.len() * (self.positions.len() - 1) / 2
        {
            return false;
        }
        let mut pairs = std::collections::BTreeSet::new();
        for p in &self.positions {
            if !name(&p.instrument)
                || !decimal(&p.notional_usd)
                || !decimal(&p.margin_usd)
                || !(-10000..=10000).contains(&p.price_shock_bps)
                || p.volatility_bps > 10000
                || p.liquidity_cost_bps > 10000
            {
                return false;
            }
        }
        for c in &self.correlations {
            if c.left >= c.right
                || c.right >= self.positions.len()
                || !c.rho.is_finite()
                || !(-1.0..=1.0).contains(&c.rho)
                || !pairs.insert((c.left, c.right))
            {
                return false;
            }
        }
        true
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticsRequest {
    pub reference: Option<String>,
    pub account: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view: Option<DiagnosticsView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cycle_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u8>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticsView {
    Finance,
    Operation,
    Research,
    Storage,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionClass {
    Read,
    IncreaseRisk,
    ReduceRisk,
    Maintenance,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandIdentity {
    pub account: String,
    pub idempotency_key: String,
    pub expires_in_ms: u64,
}
impl CommandIdentity {
    pub fn valid(&self) -> bool {
        !self.account.is_empty()
            && self.account.len() <= 128
            && (8..=128).contains(&self.idempotency_key.len())
            && self
                .idempotency_key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            && (1..=30000).contains(&self.expires_in_ms)
    }
}
fn id(v: &str) -> bool {
    (8..=128).contains(&v.len())
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn name(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 128
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b':')
}
fn decimal(v: &str) -> bool {
    positive_decimal(v).is_ok()
}
fn stable_semantic_json(value: &serde_json::Value, output: &mut String) {
    match value {
        serde_json::Value::Null => output.push_str("null"),
        serde_json::Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        serde_json::Value::Number(value) => output.push_str(&value.to_string()),
        serde_json::Value::String(value) => {
            output.push_str(&serde_json::to_string(value).expect("valid JSON string"));
        }
        serde_json::Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                stable_semantic_json(value, output);
            }
            output.push(']');
        }
        serde_json::Value::Object(values) => {
            output.push('{');
            let mut ordered = values.iter().collect::<Vec<_>>();
            ordered.sort_by_key(|(left, _)| *left);
            for (index, (key, value)) in ordered.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                output.push_str(&serde_json::to_string(key).expect("valid JSON key"));
                output.push(':');
                stable_semantic_json(value, output);
            }
            output.push('}');
        }
    }
}

impl TradeRequest {
    /// The request's immutable semantic fields, independent of policy,
    /// freshness, request IDs and the current resolved strategy revision.
    /// Typed deserialization has already applied the declared optional defaults.
    pub fn canonical_semantic(&self) -> Result<Self, DecimalInputError> {
        let mut request = self.clone();
        if let Self::Execute {
            sizing,
            order,
            protection,
            ..
        } = &mut request
        {
            let size = match sizing {
                Sizing::Contracts { contracts } => contracts,
                Sizing::MaxLossUsd { max_loss_usd } => max_loss_usd,
                Sizing::EntryBudgetUsd { entry_budget_usd } => entry_budget_usd,
            };
            *size = positive_decimal(size)?;
            if let Some(limit) = &mut order.limit_price {
                *limit = positive_decimal(limit)?;
            }
            if let Some(upper) = &mut order.max_entry_usd {
                *upper = positive_decimal(upper)?;
            }
            if let Some(Protection::Price { stop_price }) = protection {
                *stop_price = positive_decimal(stop_price)?;
            }
        }
        Ok(request)
    }

    /// Canonical UTF-8 preimage for SHA256 of the semantic trade request.
    /// This deliberately excludes resolved policy and source revisions.
    pub fn canonical_semantic_json(&self, account_key: &str) -> Result<String, DecimalInputError> {
        let normalized = self.canonical_semantic()?;
        let envelope = serde_json::json!({
            "schema": "okx.trade.semantic/v1",
            "account_key": account_key,
            "request": normalized
        });
        let mut output = String::new();
        stable_semantic_json(&envelope, &mut output);
        Ok(output)
    }
}

impl Sizing {
    pub fn valid(&self) -> bool {
        match self {
            Self::Contracts { contracts } => decimal(contracts),
            Self::MaxLossUsd { max_loss_usd } => decimal(max_loss_usd),
            Self::EntryBudgetUsd { entry_budget_usd } => decimal(entry_budget_usd),
        }
    }
}
impl Protection {
    pub fn valid(&self) -> bool {
        match self {
            Self::Price { stop_price } => decimal(stop_price),
            Self::Distance { stop_distance_bps } => (1..=10000).contains(stop_distance_bps),
        }
    }
}
impl OrderConstraints {
    pub fn valid(&self) -> bool {
        matches!(
            self.order_type.as_str(),
            "limit" | "market" | "fok" | "ioc" | "post_only"
        ) && self.reference.as_ref().is_none_or(|v| name(v))
            && self.max_entry_usd.as_ref().is_none_or(|v| decimal(v))
            && self.max_slippage_bps <= 10000
            && self.limit_price.as_ref().is_none_or(|v| decimal(v))
            && (!matches!(self.order_type.as_str(), "limit" | "post_only")
                || self.limit_price.is_some())
    }
}
impl TradeRequest {
    pub fn valid(&self) -> bool {
        match self {
            Self::Capabilities {} | Self::Preflight {} => true,
            Self::Preview {
                operation,
                strategy_instance,
                instrument,
                sizing,
                order,
                protection,
                ..
            }
            | Self::Execute {
                operation,
                strategy_instance,
                instrument,
                sizing,
                order,
                protection,
                ..
            } => {
                let common = name(strategy_instance)
                    && name(instrument)
                    && instrument.len() >= 3
                    && sizing.valid()
                    && order.valid()
                    && protection.as_ref().is_none_or(Protection::valid)
                    && (!matches!(
                        operation,
                        TradeOperation::Open | TradeOperation::Add | TradeOperation::Replace
                    ) || protection.is_some());
                common
                    && match self {
                        Self::Execute {
                            idempotency_key,
                            expires_in_ms,
                            ..
                        } => id(idempotency_key) && (1..=30000).contains(expires_in_ms),
                        _ => true,
                    }
            }
            Self::Status { reference } => name(reference),
            Self::Cancel {
                idempotency_key,
                reference,
            } => id(idempotency_key) && name(reference),
            Self::Close {
                idempotency_key,
                reference,
                expires_in_ms,
            } => id(idempotency_key) && name(reference) && (1..=30000).contains(expires_in_ms),
        }
    }
}
/// Strict typed v1 live strategy settings. JSON is only an API/SQL wire format.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyParametersV1 {
    #[serde(default = "default_fast")]
    pub fast: usize,
    #[serde(default = "default_slow")]
    pub slow: usize,
}
fn default_fast() -> usize {
    10
}
fn default_slow() -> usize {
    20
}
impl StrategyParametersV1 {
    pub fn valid(&self) -> bool {
        self.fast >= 1 && self.fast < self.slow && self.slow <= 1000
    }
    pub fn parse(value: &serde_json::Value) -> Option<Self> {
        let parsed: Self = serde_json::from_value(value.clone()).ok()?;
        parsed.valid().then_some(parsed)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyConfigurationV1 {
    pub parameters: StrategyParametersV1,
    pub instruments: Vec<String>,
    pub timeframe_ms: u64,
}

/// Version-pinned graph strategy configuration. The graph is pure data;
/// Research alone resolves operator semantics. No implicit trade activation.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum GraphExecutionPolicy {
    /// Existing graph strategy Start means read-only observation by default.
    #[default]
    Observe,
    /// Explicitly supplied order risk parameters. This is still subject to
    /// Engine account authority, the strategy budget, RiskBook and venue gates.
    Trade {
        max_slippage_bps: u32,
        stop_distance_bps: u32,
    },
}

impl GraphExecutionPolicy {
    pub fn valid(&self) -> bool {
        match self {
            Self::Observe => true,
            Self::Trade {
                max_slippage_bps,
                stop_distance_bps,
            } => (1..=100).contains(max_slippage_bps) && (1..=5_000).contains(stop_distance_bps),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphStrategyParametersInput {
    signal: crate::research_ipc::GraphSignalDefinition,
    execution: GraphExecutionPolicy,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphStrategyConfigurationV1 {
    pub signal: crate::research_ipc::GraphSignalDefinition,
    pub instruments: Vec<String>,
    pub timeframe_ms: u64,
    #[serde(default)]
    pub execution: GraphExecutionPolicy,
}
impl GraphStrategyConfigurationV1 {
    pub const DEFINITION_ID: &'static str = "graph_signal_v1";

    pub fn valid(&self) -> bool {
        self.signal.structurally_valid()
            && self.execution.valid()
            && !self.instruments.is_empty()
            && self.instruments.len() <= 32
            && self.instruments.iter().all(|v| name(v))
            && self.timeframe_ms >= 1000
    }
    pub fn parse_signal(
        value: &serde_json::Value,
    ) -> Option<crate::research_ipc::GraphSignalDefinition> {
        let parsed: crate::research_ipc::GraphSignalDefinition =
            serde_json::from_value(value.clone()).ok()?;
        parsed.structurally_valid().then_some(parsed)
    }

    /// Legacy graph parameters remain observation-only. An executable graph
    /// requires an explicit, strictly typed `signal` + `execution` wrapper.
    pub fn parse_parameters(
        value: &serde_json::Value,
    ) -> Option<(
        crate::research_ipc::GraphSignalDefinition,
        GraphExecutionPolicy,
    )> {
        if value.get("signal").is_none() {
            return Some((Self::parse_signal(value)?, GraphExecutionPolicy::Observe));
        }
        let input: GraphStrategyParametersInput = serde_json::from_value(value.clone()).ok()?;
        (input.signal.structurally_valid() && input.execution.valid())
            .then_some((input.signal, input.execution))
    }
}
impl StrategyConfigurationV1 {
    pub fn valid(&self) -> bool {
        self.parameters.valid()
            && !self.instruments.is_empty()
            && self.instruments.len() <= 32
            && self.instruments.iter().all(|v| name(v))
            && self.timeframe_ms >= 1000
    }
}
#[cfg(test)]
mod typed_strategy_parameters_tests {
    use super::*;
    #[test]
    fn graph_execution_requires_explicit_typed_trade_policy() {
        let signal = serde_json::json!({
            "version":"okx.graph-signal/v1",
            "graph":{"schema":"okx.graph/v1","steps":[
                {"id":"r","op":{"id":"returns_series","version":1},
                 "inputs":[{"source":"close"}]},
                {"id":"p","op":{"id":"greater_than","version":1},
                 "inputs":[{"step":"r"}],"params":{"threshold":0.0}}
            ],"outputs":["p"]},
            "signal_step":"p","target_if_true":1,"target_if_false":0,"missing":"hold"
        });
        let (legacy, mode) = GraphStrategyConfigurationV1::parse_parameters(&signal).unwrap();
        assert_eq!(mode, GraphExecutionPolicy::Observe);
        assert!(legacy.structurally_valid());
        let request = serde_json::json!({
            "signal":signal,
            "execution":{"mode":"trade","max_slippage_bps":5,"stop_distance_bps":50}
        });
        let (_, mode) = GraphStrategyConfigurationV1::parse_parameters(&request).unwrap();
        assert_eq!(
            mode,
            GraphExecutionPolicy::Trade {
                max_slippage_bps: 5,
                stop_distance_bps: 50,
            }
        );
        for invalid in [
            serde_json::json!({"mode":"trade","max_slippage_bps":0,"stop_distance_bps":50}),
            serde_json::json!({"mode":"trade","max_slippage_bps":5,"stop_distance_bps":0}),
            serde_json::json!({"mode":"trade","max_slippage_bps":5,"stop_distance_bps":50,"extra":true}),
        ] {
            let mut invalid_request = request.clone();
            invalid_request["execution"] = invalid;
            assert!(GraphStrategyConfigurationV1::parse_parameters(&invalid_request).is_none());
        }
    }

    #[test]
    fn s2_06_same_golden_preimage_and_sha256_for_reordered_semantics() {
        use sha2::{Digest, Sha256};
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../contracts/trade-semantic-golden.json"
        ))
        .unwrap();
        let account = fixture["trade"]["account_key"].as_str().unwrap();
        let expected_json = fixture["trade"]["canonical_utf8_json"].as_str().unwrap();
        let expected_hash = fixture["trade"]["sha256"].as_str().unwrap();
        for input in fixture["trade"]["variants"].as_array().unwrap() {
            let request: TradeRequest = serde_json::from_value(input.clone()).unwrap();
            let canonical = request.canonical_semantic_json(account).unwrap();
            assert_eq!(canonical, expected_json);
            let sha = format!("{:x}", Sha256::digest(canonical.as_bytes()));
            assert_eq!(sha, expected_hash);
        }
    }

    #[test]
    fn s2_01_execute_semantic_decimal_equivalence() {
        let raw = r#"{"action":"execute","idempotency_key":"numeric_semantic_01","operation":"open","strategy_instance":"manual","instrument":"BTC-USDT-SWAP","side":"long","sizing":{"max_loss_usd":"5.0"},"order":{"type":"market","limit_price":null,"reference":null,"max_slippage_bps":5},"protection":{"stop_price":"900.00"},"expires_in_ms":10000}"#;
        let first: TradeRequest = serde_json::from_str(raw).unwrap();
        let second: TradeRequest =
            serde_json::from_str(&raw.replace("5.0", "5.00").replace("900.00", "900")).unwrap();
        assert_eq!(
            first.canonical_semantic().unwrap(),
            second.canonical_semantic().unwrap()
        );
        let canonical = first.canonical_semantic().unwrap();
        match canonical {
            TradeRequest::Execute {
                sizing, protection, ..
            } => {
                assert_eq!(
                    sizing,
                    Sizing::MaxLossUsd {
                        max_loss_usd: "5".into()
                    }
                );
                assert_eq!(
                    protection,
                    Some(Protection::Price {
                        stop_price: "900".into()
                    })
                );
            }
            _ => panic!("expected execute"),
        }
    }

    #[test]
    fn live_parameters_v1_fail_closed_and_bound_default() {
        let valid = serde_json::json!({"fast":20,"slow":50});
        assert_eq!(StrategyParametersV1::parse(&valid).unwrap().fast, 20);
        assert!(StrategyParametersV1::parse(&serde_json::json!({})).is_some());
        for bad in [
            serde_json::json!({"fast":51,"slow":50}),
            serde_json::json!({"fast":0,"slow":50}),
            serde_json::json!({"fast":1,"slow":1001}),
            serde_json::json!({"fast":"20","slow":50}),
            serde_json::json!({"fast":20,"slow":50,"unsafe_override":true}),
        ] {
            assert!(StrategyParametersV1::parse(&bad).is_none());
        }
    }
}
impl StrategyRequest {
    pub fn valid(&self) -> bool {
        match self {
            Self::List {} => true,
            Self::Status { instance_id } => name(instance_id),
            Self::CapitalAssign {
                idempotency_key,
                instance_id,
                amount_usd,
                expected_authority_revision,
                ..
            } => {
                id(idempotency_key)
                    && name(instance_id)
                    && instance_id != "manual"
                    && decimal(amount_usd)
                    && *expected_authority_revision > 0
            }
            Self::Start {
                idempotency_key,
                instance_id,
            }
            | Self::Stop {
                idempotency_key,
                instance_id,
            } => id(idempotency_key) && name(instance_id),
            Self::Create {
                idempotency_key,
                instance_id,
                definition_id,
                version,
                parameters,
                instruments,
                timeframe_ms,
                budget_usd,
            } => {
                id(idempotency_key)
                    && name(instance_id)
                    && name(definition_id)
                    && *version == 1
                    && (if definition_id == GraphStrategyConfigurationV1::DEFINITION_ID {
                        GraphStrategyConfigurationV1::parse_signal(parameters).is_some()
                            && parameters.to_string().len() <= 60 * 1024
                    } else {
                        StrategyParametersV1::parse(parameters).is_some()
                            && parameters.to_string().len() <= 4096
                    })
                    && !instruments.is_empty()
                    && instruments.len() <= 32
                    && instruments.iter().all(|v| name(v))
                    && *timeframe_ms >= 1000
                    && decimal(budget_usd)
            }
            Self::Update {
                idempotency_key,
                instance_id,
                parameters,
                budget_usd,
                ..
            } => {
                id(idempotency_key)
                    && name(instance_id)
                    && ((GraphStrategyConfigurationV1::parse_signal(parameters).is_some()
                        && parameters.to_string().len() <= 60 * 1024)
                        || (StrategyParametersV1::parse(parameters).is_some()
                            && parameters.to_string().len() <= 4096))
                    && decimal(budget_usd)
            }
        }
    }
}
impl PortfolioRequest {
    pub fn valid(&self) -> bool {
        !self.accounts.is_empty()
            && self.accounts.len() <= 32
            && self.accounts.iter().all(|v| name(v))
            && self.end_ms > self.start_ms
            && (1..=50).contains(&self.limit)
            && self.cursor.as_ref().is_none_or(|v| v.len() <= 512)
            && self
                .strategies
                .as_ref()
                .is_none_or(|v| v.len() <= 32 && v.iter().all(|v| name(v)))
            && self
                .currencies
                .as_ref()
                .is_none_or(|v| v.len() <= 32 && v.iter().all(|v| name(v)))
    }
}
impl DiagnosticsRequest {
    pub fn valid(&self) -> bool {
        self.reference.as_ref().is_some_and(|v| name(v))
            || self.account.as_ref().is_some_and(|v| name(v))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn graph_strategy_accepts_typed_composition_and_rejects_legacy_ema_parameters() {
        let graph = serde_json::json!({
            "version":"okx.graph-signal/v1",
            "graph":{"schema":"okx.graph/v1","steps":[
                {"id":"positive","op":{"id":"greater_than","version":1},
                 "inputs":[{"source":"close"}],"params":{"threshold":100}}
            ],"outputs":["positive"]},
            "signal_step":"positive","target_if_true":1,"target_if_false":0,"missing":"hold"
        });
        let create = StrategyRequest::Create {
            idempotency_key: "graph_contract_create".into(),
            instance_id: "graph-btc".into(),
            definition_id: GraphStrategyConfigurationV1::DEFINITION_ID.into(),
            version: 1,
            parameters: graph.clone(),
            instruments: vec!["BTC-USDT-SWAP".into()],
            timeframe_ms: 60_000,
            budget_usd: "2".into(),
        };
        assert!(create.valid());
        let mut legacy_payload = create.clone();
        if let StrategyRequest::Create { parameters, .. } = &mut legacy_payload {
            *parameters = serde_json::json!({"fast": 5, "slow": 10});
        }
        assert!(!legacy_payload.valid());
        let mut invalid_step = create;
        if let StrategyRequest::Create { parameters, .. } = &mut invalid_step {
            parameters["graph"]["steps"][0]["op"]["version"] = serde_json::json!(0);
        }
        assert!(!invalid_step.valid());
        let update = StrategyRequest::Update {
            idempotency_key: "graph_contract_update".into(),
            instance_id: "graph-btc".into(),
            expected_revision: 1,
            parameters: graph,
            budget_usd: "2".into(),
        };
        assert!(update.valid());
    }
    #[test]
    fn trade_positive_negative_wire_fixtures() {
        let good = r#"{"action":"execute","operation":"open","strategy_instance":"manual","idempotency_key":"command_001","instrument":"BTC-USDT-SWAP","side":"long","sizing":{"max_loss_usd":"5.00"},"order":{"type":"fok","max_slippage_bps":5},"protection":{"stop_distance_bps":50},"expires_in_ms":10000}"#;
        let parsed: TradeRequest = serde_json::from_str(good).unwrap();
        assert!(parsed.valid());
        for bad in [
            good.replace("10000}", "30001}"),
            good.replace("\"5.00\"", "\"-5\""),
            good.replace("command_001", "x"),
        ] {
            assert!(!serde_json::from_str::<TradeRequest>(&bad).unwrap().valid());
        }
        let mut bad: serde_json::Value = serde_json::from_str(good).unwrap();
        bad["sizing"]["contracts"] = serde_json::json!("1");
        assert!(serde_json::from_value::<TradeRequest>(bad).is_err());
    }
}

#[cfg(test)]
mod capital_assignment_tests {
    use super::*;
    #[test]
    fn capital_assignment_is_strict_and_never_manual_nav() {
        let input = r#"{"action":"capital_assign","idempotency_key":"capital_assignment_01","instance_id":"ema","direction":"assign","amount_usd":"10.25","expected_authority_revision":1}"#;
        let request: StrategyRequest = serde_json::from_str(input).unwrap();
        assert!(request.valid());
        let release = input.replace("\"assign\"", "\"release\"");
        assert!(
            serde_json::from_str::<StrategyRequest>(&release)
                .unwrap()
                .valid()
        );
        for bad in [
            input.replace("\"10.25\"", "\"0\""),
            input.replace("\"10.25\"", "\"-1\""),
            input.replace("\"ema\"", "\"manual\""),
            input.replace("revision\":1", "revision\":0"),
        ] {
            assert!(
                !serde_json::from_str::<StrategyRequest>(&bad)
                    .unwrap()
                    .valid()
            );
        }
        assert!(
            serde_json::from_str::<StrategyRequest>(&input.replace("\"assign\"", "\"unverified\""))
                .is_err()
        );
    }
}

#[cfg(test)]
#[path = "trading_property_tests.rs"]
mod trading_property_tests;

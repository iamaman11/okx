//! Read-only Research-publication -> Engine-target boundary.
//!
//! This is not an execution consumer. A persisted Engine binding, durable
//! publication cursor and atomic decision claim are prerequisites to routing
//! any candidate to `TradeService::execute_for_definition` or `close`.

use super::execute::GraphOpenContext;
use super::{TradeRequest, TradeService};
use okx_execution::{
    StrategyInstance, StrategyPositionSnapshot,
    graph_binding::{EngineGraphBinding, EngineGraphCursor, GraphIdleAck},
    position_target::{TargetPlan, TargetPlanError, TargetPosition, plan_strategy_target},
};
use okx_protocol::research_ipc::{
    GraphFeedRequest, GraphPosition, GraphPublicationCursor, GraphSessionId, GraphSignalIntent,
    GraphSignalPublication,
};
use okx_protocol::trading::GraphStrategyConfigurationV1;
use okx_protocol::trading::{
    GraphExecutionPolicy, OrderConstraints, PositionSide, Protection, Sizing, TradeOperation,
};
use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphTargetBinding {
    pub account_key: String,
    pub instance_id: String,
    pub definition_version: String,
    pub policy_revision: i64,
    pub session_id: GraphSessionId,
    pub graph_sha256: String,
    pub instrument_id: String,
    pub timeframe_ms: u64,
}

impl GraphTargetBinding {
    /// Construct a candidate for the ORIGINAL TradeService path. This has no
    /// SQL, risk or venue effects. Observe-only strategy Start cannot create
    /// an order command, including after an executable upgrade.
    pub(super) fn open_request(
        &self,
        instance: &StrategyInstance,
        publication: &GraphSignalPublication,
        policy: &GraphExecutionPolicy,
    ) -> Result<Option<TradeRequest>, String> {
        let GraphExecutionPolicy::Trade {
            max_slippage_bps,
            stop_distance_bps,
        } = policy
        else {
            return Ok(None);
        };
        if !policy.valid()
            || !instance.enabled
            || instance.definition_id != GraphStrategyConfigurationV1::DEFINITION_ID
            || instance.instance_id != self.instance_id
            || instance.definition_version != self.definition_version
            || instance.policy_revision != self.policy_revision
            || instance.budget <= Decimal::ZERO
        {
            return Err("GRAPH_OPEN_POLICY_OR_STRATEGY_INVALID".into());
        }
        let side = match publication.intent {
            GraphSignalIntent::SetTarget(GraphPosition::Long) => PositionSide::Long,
            GraphSignalIntent::SetTarget(GraphPosition::Short) => PositionSide::Short,
            _ => return Err("GRAPH_OPEN_ACTION_REQUIRED".into()),
        };
        let cursor = EngineGraphCursor {
            sequence: publication.sequence,
            closed_ms: publication.closed_ms,
            intent: if side == PositionSide::Long {
                TargetPosition::Long
            } else {
                TargetPosition::Short
            },
        };
        let identity = self
            .storage_binding()
            .open_identity(&cursor)
            .ok_or("GRAPH_OPEN_IDENTITY_INVALID")?;
        let request = TradeRequest::Execute {
            idempotency_key: identity.decision_key,
            operation: TradeOperation::Open,
            strategy_instance: self.instance_id.clone(),
            instrument: self.instrument_id.clone(),
            side,
            sizing: Sizing::MaxLossUsd {
                // This is only the instance's already validated risk ceiling;
                // the current campaign/account ceiling is checked by Engine.
                max_loss_usd: instance.budget.normalize().to_string(),
            },
            order: OrderConstraints {
                order_type: "fok".into(),
                limit_price: None,
                reference: None,
                max_entry_usd: None,
                max_slippage_bps: *max_slippage_bps,
            },
            protection: Some(Protection::Distance {
                stop_distance_bps: *stop_distance_bps,
            }),
            expires_in_ms: 10_000,
        };
        self.open_context(publication, &request)?;
        Ok(Some(request))
    }

    /// Reconstruct the typed Engine binding from original finance SQL after
    /// a process restart; never trust a broadcast frame as activation state.
    pub(super) fn from_storage(stored: EngineGraphBinding) -> Result<Self, String> {
        if !stored.valid() {
            return Err("GRAPH_TARGET_STORED_BINDING_INVALID".into());
        }
        let binding = Self {
            account_key: stored.account_key,
            instance_id: stored.instance_id,
            definition_version: stored.definition_version,
            policy_revision: stored.policy_revision,
            session_id: GraphSessionId::new(stored.session_id).map_err(str::to_owned)?,
            graph_sha256: stored.graph_sha256,
            instrument_id: stored.instrument_id,
            timeframe_ms: stored.timeframe_ms,
        };
        if !binding.valid() {
            return Err("GRAPH_TARGET_STORED_BINDING_INVALID".into());
        }
        Ok(binding)
    }

    pub fn valid(&self) -> bool {
        !self.account_key.is_empty()
            && !self.instance_id.is_empty()
            && !self.definition_version.is_empty()
            && self.policy_revision > 0
            && (GraphFeedRequest::Start {
                session_id: self.session_id.clone(),
                graph_sha256: self.graph_sha256.clone(),
                instrument: self.instrument_id.clone(),
                timeframe_ms: self.timeframe_ms,
                poll_every_ms: 1_000,
            })
            .valid()
    }

    pub(super) fn storage_binding(&self) -> EngineGraphBinding {
        EngineGraphBinding {
            account_key: self.account_key.clone(),
            instance_id: self.instance_id.clone(),
            definition_version: self.definition_version.clone(),
            policy_revision: self.policy_revision,
            session_id: self.session_id.as_str().into(),
            graph_sha256: self.graph_sha256.clone(),
            instrument_id: self.instrument_id.clone(),
            timeframe_ms: self.timeframe_ms,
        }
    }

    pub(super) fn storage_cursor(cursor: &GraphPublicationCursor) -> EngineGraphCursor {
        EngineGraphCursor {
            sequence: cursor.last_sequence(),
            closed_ms: cursor.last_closed_ms(),
            intent: match cursor.last_intent() {
                GraphSignalIntent::Hold => TargetPosition::Hold,
                GraphSignalIntent::SetTarget(GraphPosition::Flat) => TargetPosition::Flat,
                GraphSignalIntent::SetTarget(GraphPosition::Long) => TargetPosition::Long,
                GraphSignalIntent::SetTarget(GraphPosition::Short) => TargetPosition::Short,
            },
        }
    }

    fn live_cursor(&self, stored: &EngineGraphCursor) -> Result<GraphPublicationCursor, String> {
        let intent = match stored.intent {
            TargetPosition::Hold => GraphSignalIntent::Hold,
            TargetPosition::Flat => GraphSignalIntent::SetTarget(GraphPosition::Flat),
            TargetPosition::Long => GraphSignalIntent::SetTarget(GraphPosition::Long),
            TargetPosition::Short => GraphSignalIntent::SetTarget(GraphPosition::Short),
        };
        GraphPublicationCursor::from_checkpoint(
            self.session_id.clone(),
            self.graph_sha256.clone(),
            stored.closed_ms,
            stored.sequence,
            intent,
        )
        .map_err(str::to_owned)
    }

    /// Only a correctly scoped Exchange Execute can be linked to a graph
    /// publication. This check is repeated atomically at the finance owner.
    fn open_context(
        &self,
        publication: &GraphSignalPublication,
        request: &TradeRequest,
    ) -> Result<GraphOpenContext, String> {
        if !self.valid()
            || !publication.valid()
            || publication.session_id != self.session_id
            || publication.graph_sha256 != self.graph_sha256
            || publication.closed_ms % self.timeframe_ms != 0
        {
            return Err("GRAPH_OPEN_PUBLICATION_MISMATCH".into());
        }
        let (target, expected_side) = match publication.intent {
            GraphSignalIntent::SetTarget(GraphPosition::Long) => {
                (TargetPosition::Long, PositionSide::Long)
            }
            GraphSignalIntent::SetTarget(GraphPosition::Short) => {
                (TargetPosition::Short, PositionSide::Short)
            }
            GraphSignalIntent::Hold | GraphSignalIntent::SetTarget(GraphPosition::Flat) => {
                return Err("GRAPH_OPEN_ACTION_REQUIRED".into());
            }
        };
        let TradeRequest::Execute {
            idempotency_key,
            operation,
            strategy_instance,
            instrument,
            side,
            ..
        } = request
        else {
            return Err("GRAPH_OPEN_EXECUTE_REQUIRED".into());
        };
        if *operation != TradeOperation::Open
            || *side != expected_side
            || *strategy_instance != self.instance_id
            || *instrument != self.instrument_id
        {
            return Err("GRAPH_OPEN_REQUEST_SCOPE_MISMATCH".into());
        }
        let binding = self.storage_binding();
        let published = EngineGraphCursor {
            sequence: publication.sequence,
            closed_ms: publication.closed_ms,
            intent: target,
        };
        let identity = binding
            .open_identity(&published)
            .ok_or("GRAPH_OPEN_IDENTITY_INVALID")?;
        if *idempotency_key != identity.decision_key {
            return Err("GRAPH_OPEN_IDEMPOTENCY_MISMATCH".into());
        }
        Ok(GraphOpenContext {
            binding,
            publication: published,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphTargetError {
    InvalidBinding,
    StrategyStoppedOrChanged,
    PublicationMismatch,
    CursorInvalid(&'static str),
    Plan(TargetPlanError),
}

/// A proposed decision without a financial grant, mutation or durable ACK.
/// The cursor is a *candidate*; publishing this struct does not commit it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct GraphTargetPreview {
    pub next_cursor: GraphPublicationCursor,
    pub target: TargetPosition,
    pub plan: TargetPlan,
}

impl GraphTargetBinding {
    /// Validate one publication against an exact Engine-owned account/strategy
    /// snapshot and an already confirmed cursor, leaving both unchanged.
    /// Repeated identical events do not cause another proposal.
    pub fn preview(
        &self,
        snapshot: &StrategyPositionSnapshot,
        last_accepted: &GraphPublicationCursor,
        publication: &GraphSignalPublication,
    ) -> Result<Option<GraphTargetPreview>, GraphTargetError> {
        if !self.valid() {
            return Err(GraphTargetError::InvalidBinding);
        }
        if !snapshot.instance.enabled
            || snapshot.instance.instance_id != self.instance_id
            || snapshot.instance.definition_version != self.definition_version
            || snapshot.instance.policy_revision != self.policy_revision
        {
            return Err(GraphTargetError::StrategyStoppedOrChanged);
        }
        if !publication.valid()
            || publication.session_id != self.session_id
            || publication.graph_sha256 != self.graph_sha256
            || publication.closed_ms % self.timeframe_ms != 0
        {
            return Err(GraphTargetError::PublicationMismatch);
        }
        let mut next_cursor = last_accepted.clone();
        if !next_cursor
            .accept(publication)
            .map_err(GraphTargetError::CursorInvalid)?
        {
            return Ok(None);
        }
        let target = match publication.intent {
            GraphSignalIntent::Hold => TargetPosition::Hold,
            GraphSignalIntent::SetTarget(GraphPosition::Flat) => TargetPosition::Flat,
            GraphSignalIntent::SetTarget(GraphPosition::Long) => TargetPosition::Long,
            GraphSignalIntent::SetTarget(GraphPosition::Short) => TargetPosition::Short,
        };
        let plan = plan_strategy_target(
            target,
            &self.instance_id,
            &self.instrument_id,
            &snapshot.lots,
            snapshot.in_flight,
        )
        .map_err(GraphTargetError::Plan)?;
        Ok(Some(GraphTargetPreview {
            next_cursor,
            target,
            plan,
        }))
    }
}

impl TradeService {
    /// Staging-only execution entry: the original TradeService risk/venue
    /// pipeline is reused, but its first finance PREPARE is graph-atomic.
    /// The background Research consumer is not wired in this change.
    pub(crate) async fn execute_graph_open(
        &self,
        account: &str,
        binding: &GraphTargetBinding,
        publication: &GraphSignalPublication,
        request: &TradeRequest,
    ) -> Result<serde_json::Value, String> {
        let context = binding.open_context(publication, request)?;
        let confirmed = self
            .graph_confirmed_cursor(account, binding)
            .await?
            .ok_or("GRAPH_FINANCE_BINDING_MISSING")?;
        let preview = self
            .preview_graph_publication(account, binding, &confirmed, publication)
            .await?
            .ok_or("GRAPH_OPEN_ALREADY_CONFIRMED")?;
        if !matches!(preview.plan, TargetPlan::Open(_)) {
            return Err("GRAPH_OPEN_REQUIRES_EMPTY_EXPOSURE".into());
        }
        let resources = self.resources(account).await?;
        if resources.configuration.account_key.wire() != binding.account_key {
            return Err("GRAPH_OPEN_ACCOUNT_MISMATCH".into());
        }
        self.execute_for_graph_open(&resources, request, context)
            .await
    }

    /// Persist one exact Engine activation identity after Research Restore.
    /// Repeated calls recover the confirmed SQL cursor, never rewind it.
    /// This is an internal Engine stage, not a new public trade command.
    pub async fn bind_graph_strategy(
        &self,
        account: &str,
        binding: &GraphTargetBinding,
        restored: &GraphPublicationCursor,
    ) -> Result<GraphPublicationCursor, String> {
        if !binding.valid() || !restored.matches_binding(&binding.session_id, &binding.graph_sha256)
        {
            return Err("GRAPH_BINDING_RESTORE_MISMATCH".into());
        }
        let resources = self.resources(account).await?;
        if resources.configuration.account_key.wire() != binding.account_key {
            return Err("GRAPH_TARGET_ACCOUNT_MISMATCH".into());
        }
        let instance = resources
            .execution
            .store
            .strategy_instances(super::deadline())
            .await
            .map_err(super::err)?
            .into_iter()
            .find(|instance| instance.instance_id == binding.instance_id)
            .ok_or("GRAPH_TARGET_STRATEGY_MISSING")?;
        if instance.definition_id != GraphStrategyConfigurationV1::DEFINITION_ID {
            return Err("GRAPH_TARGET_STRATEGY_MODE_MISMATCH".into());
        }
        let configuration: GraphStrategyConfigurationV1 =
            serde_json::from_str(&instance.parameters)
                .map_err(|_| "GRAPH_TARGET_CONFIGURATION_CORRUPT")?;
        if !configuration.valid()
            || configuration.timeframe_ms != binding.timeframe_ms
            || !configuration.instruments.contains(&binding.instrument_id)
        {
            return Err("GRAPH_TARGET_CONFIGURATION_MISMATCH".into());
        }
        let progress = resources
            .execution
            .store
            .bind_graph(
                super::deadline(),
                binding.storage_binding(),
                GraphTargetBinding::storage_cursor(restored),
                super::now(),
            )
            .await
            .map_err(super::err)?;
        binding.live_cursor(&progress.cursor)
    }

    /// Recover from the original finance owner after Engine or Research
    /// restarts. An absent or stale binding cannot synthesize a fresh signal.
    pub async fn graph_confirmed_cursor(
        &self,
        account: &str,
        binding: &GraphTargetBinding,
    ) -> Result<Option<GraphPublicationCursor>, String> {
        if !binding.valid() {
            return Err("GRAPH_TARGET_BINDING_INVALID".into());
        }
        let resources = self.resources(account).await?;
        if resources.configuration.account_key.wire() != binding.account_key {
            return Err("GRAPH_TARGET_ACCOUNT_MISMATCH".into());
        }
        resources
            .execution
            .store
            .graph_progress(super::deadline(), binding.storage_binding())
            .await
            .map_err(super::err)?
            .map(|state| binding.live_cursor(&state.cursor))
            .transpose()
    }

    /// A durable ACK for Hold/already-satisfied targets only. The SQL writer
    /// rechecks the active strategy and original position snapshot atomically.
    /// Financially actionable events are never ACKed on this route.
    pub async fn acknowledge_graph_idle(
        &self,
        account: &str,
        binding: &GraphTargetBinding,
        publication: &GraphSignalPublication,
    ) -> Result<GraphIdleAck, String> {
        let resources = self.resources(account).await?;
        if !binding.valid() || resources.configuration.account_key.wire() != binding.account_key {
            return Err("GRAPH_TARGET_BINDING_INVALID".into());
        }
        let confirmed = self
            .graph_confirmed_cursor(account, binding)
            .await?
            .ok_or("GRAPH_FINANCE_BINDING_MISSING")?;
        let proposed = self
            .preview_graph_publication(account, binding, &confirmed, publication)
            .await?;
        if proposed
            .as_ref()
            .is_some_and(|preview| preview.plan != TargetPlan::NoChange)
        {
            return Err("GRAPH_FINANCE_ATOMIC_INTENT_REQUIRED".into());
        }
        let next = proposed.map_or_else(
            || GraphTargetBinding::storage_cursor(&confirmed),
            |preview| GraphTargetBinding::storage_cursor(&preview.next_cursor),
        );
        resources
            .execution
            .store
            .acknowledge_graph_idle(
                super::deadline(),
                binding.storage_binding(),
                next,
                super::now(),
            )
            .await
            .map_err(super::err)
    }

    /// Engine's original financial owner supplies one scoped and consistent
    /// strategy/lots/in-flight read. This API is diagnostic and cannot place
    /// orders or advance the confirmed cursor.
    pub async fn preview_graph_publication(
        &self,
        account: &str,
        binding: &GraphTargetBinding,
        confirmed: &GraphPublicationCursor,
        publication: &GraphSignalPublication,
    ) -> Result<Option<GraphTargetPreview>, String> {
        if !binding.valid() {
            return Err("GRAPH_TARGET_BINDING_INVALID".into());
        }
        let resources = self.resources(account).await?;
        if resources.configuration.account_key.wire() != binding.account_key {
            return Err("GRAPH_TARGET_ACCOUNT_MISMATCH".into());
        }
        let snapshot = resources
            .execution
            .store
            .strategy_position_snapshot(
                super::deadline(),
                binding.instance_id.clone(),
                binding.instrument_id.clone(),
            )
            .await
            .map_err(super::err)?;
        binding
            .preview(&snapshot, confirmed, publication)
            .map_err(|error| format!("GRAPH_TARGET_PREVIEW_{error:?}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_execution::position_target::ExposureSide;
    use okx_execution::{StoredLot, StrategyInstance};
    use okx_protocol::research_ipc::{GraphSessionReply, GraphSignalPublication};
    use okx_protocol::trading::{OrderConstraints, Sizing};
    use rust_decimal::Decimal;

    fn fixture() -> (
        GraphTargetBinding,
        StrategyPositionSnapshot,
        GraphPublicationCursor,
        GraphSignalPublication,
    ) {
        let session_id = GraphSessionId::new("engine:strategy-a:btc".into()).unwrap();
        let graph_sha256 = "a".repeat(64);
        let binding = GraphTargetBinding {
            account_key: "account-a".into(),
            instance_id: "strategy-a".into(),
            definition_version: "version-1".into(),
            policy_revision: 7,
            session_id: session_id.clone(),
            graph_sha256: graph_sha256.clone(),
            instrument_id: "BTC-USDT-SWAP".into(),
            timeframe_ms: 60_000,
        };
        let snapshot = StrategyPositionSnapshot {
            instance: StrategyInstance {
                instance_id: binding.instance_id.clone(),
                definition_id: "graph".into(),
                definition_version: binding.definition_version.clone(),
                parameters: "{}".into(),
                enabled: true,
                policy_revision: 7,
                budget: Decimal::ONE,
                last_decision_key: None,
            },
            lots: Vec::new(),
            in_flight: false,
        };
        let restored = GraphSessionReply::Evaluated {
            schema: GraphSessionReply::SCHEMA.into(),
            session_id: session_id.clone(),
            graph_sha256: graph_sha256.clone(),
            last_closed_ms: 120_000,
            observed_bars: 2,
            intent: GraphSignalIntent::SetTarget(GraphPosition::Long),
        };
        let cursor = GraphPublicationCursor::from_restored(&restored).unwrap();
        let published = GraphSignalPublication {
            schema: GraphSessionReply::SCHEMA.into(),
            session_id,
            graph_sha256,
            closed_ms: 180_000,
            sequence: 3,
            intent: GraphSignalIntent::SetTarget(GraphPosition::Flat),
        };
        (binding, snapshot, cursor, published)
    }

    fn owned_long() -> StoredLot {
        StoredLot {
            lot_id: "owned-lot".into(),
            instance_id: Some("strategy-a".into()),
            intent_id: Some("original-entry".into()),
            fill_id: None,
            instrument_id: "BTC-USDT-SWAP".into(),
            side: "long".into(),
            remaining_qty: Decimal::ONE,
            cost: Decimal::from(100),
            opened_at: 100_000,
        }
    }

    #[test]
    fn graph_open_request_identity_and_direction_are_bound_before_financial_preparation() {
        let (binding, _, _, mut event) = fixture();
        event.intent = GraphSignalIntent::SetTarget(GraphPosition::Long);
        let storage = binding.storage_binding();
        let publication = EngineGraphCursor {
            sequence: event.sequence,
            closed_ms: event.closed_ms,
            intent: TargetPosition::Long,
        };
        let identity = storage.open_identity(&publication).unwrap();
        let request = TradeRequest::Execute {
            idempotency_key: identity.decision_key.clone(),
            operation: TradeOperation::Open,
            strategy_instance: binding.instance_id.clone(),
            instrument: binding.instrument_id.clone(),
            side: PositionSide::Long,
            sizing: Sizing::MaxLossUsd {
                max_loss_usd: "1".into(),
            },
            order: OrderConstraints {
                order_type: "fok".into(),
                limit_price: None,
                reference: None,
                max_entry_usd: None,
                max_slippage_bps: 5,
            },
            protection: None,
            expires_in_ms: 10_000,
        };
        let accepted = binding.open_context(&event, &request).unwrap();
        assert_eq!(accepted.binding, storage);
        assert_eq!(accepted.publication, publication);
        let mut wrong_key = request.clone();
        if let TradeRequest::Execute {
            idempotency_key, ..
        } = &mut wrong_key
        {
            *idempotency_key = "foreign-graph-key".into();
        }
        assert_eq!(
            binding.open_context(&event, &wrong_key).unwrap_err(),
            "GRAPH_OPEN_IDEMPOTENCY_MISMATCH"
        );
        let mut wrong_side = request.clone();
        if let TradeRequest::Execute { side, .. } = &mut wrong_side {
            *side = PositionSide::Short;
        }
        assert_eq!(
            binding.open_context(&event, &wrong_side).unwrap_err(),
            "GRAPH_OPEN_REQUEST_SCOPE_MISMATCH"
        );
        event.intent = GraphSignalIntent::SetTarget(GraphPosition::Flat);
        assert_eq!(
            binding.open_context(&event, &request).unwrap_err(),
            "GRAPH_OPEN_ACTION_REQUIRED"
        );
    }

    #[test]
    fn graph_open_request_requires_explicit_trade_policy_and_original_identity() {
        let (binding, mut snapshot, _, mut event) = fixture();
        snapshot.instance.definition_id = GraphStrategyConfigurationV1::DEFINITION_ID.into();
        snapshot.instance.budget = Decimal::new(250, 2);
        event.intent = GraphSignalIntent::SetTarget(GraphPosition::Short);
        assert!(
            binding
                .open_request(&snapshot.instance, &event, &GraphExecutionPolicy::Observe)
                .unwrap()
                .is_none()
        );
        let policy = GraphExecutionPolicy::Trade {
            max_slippage_bps: 5,
            stop_distance_bps: 50,
        };
        let request = binding
            .open_request(&snapshot.instance, &event, &policy)
            .unwrap()
            .unwrap();
        assert!(request.valid());
        assert_eq!(
            binding.open_context(&event, &request).unwrap().binding,
            binding.storage_binding()
        );
        let TradeRequest::Execute {
            side,
            sizing,
            protection,
            ..
        } = request
        else {
            panic!("an armed graph must yield an Execute command");
        };
        assert_eq!(side, PositionSide::Short);
        assert_eq!(
            sizing,
            Sizing::MaxLossUsd {
                max_loss_usd: "2.5".into()
            }
        );
        assert_eq!(
            protection,
            Some(Protection::Distance {
                stop_distance_bps: 50
            })
        );
        let mut stale = snapshot.instance.clone();
        stale.policy_revision += 1;
        assert_eq!(
            binding.open_request(&stale, &event, &policy).unwrap_err(),
            "GRAPH_OPEN_POLICY_OR_STRATEGY_INVALID"
        );
        event.graph_sha256 = "b".repeat(64);
        assert_eq!(
            binding
                .open_request(&snapshot.instance, &event, &policy)
                .unwrap_err(),
            "GRAPH_OPEN_PUBLICATION_MISMATCH"
        );
    }

    #[test]
    fn flat_from_research_previews_close_without_mutating_engine_cursor() {
        let (binding, mut snapshot, cursor, event) = fixture();
        snapshot.lots.push(owned_long());
        let candidate = binding
            .preview(&snapshot, &cursor, &event)
            .unwrap()
            .unwrap();
        assert_eq!(candidate.target, TargetPosition::Flat);
        assert_eq!(
            candidate.plan,
            TargetPlan::CloseExisting(vec!["original-entry".into()])
        );
        assert_eq!(cursor.last_sequence(), 2);
        assert_eq!(candidate.next_cursor.last_sequence(), 3);
        assert_eq!(
            binding
                .preview(&snapshot, &candidate.next_cursor, &event)
                .unwrap(),
            None
        );
    }

    #[test]
    fn target_long_does_not_buy_again_or_flip_before_settlement() {
        let (binding, mut snapshot, cursor, mut event) = fixture();
        event.intent = GraphSignalIntent::SetTarget(GraphPosition::Long);
        snapshot.lots.push(owned_long());
        assert_eq!(
            binding
                .preview(&snapshot, &cursor, &event)
                .unwrap()
                .unwrap()
                .plan,
            TargetPlan::NoChange
        );
        snapshot.lots.clear();
        assert_eq!(
            binding
                .preview(&snapshot, &cursor, &event)
                .unwrap()
                .unwrap()
                .plan,
            TargetPlan::Open(ExposureSide::Long)
        );
        snapshot.in_flight = true;
        assert_eq!(
            binding
                .preview(&snapshot, &cursor, &event)
                .unwrap()
                .unwrap()
                .plan,
            TargetPlan::AwaitSettlement
        );
    }

    #[test]
    fn stale_strategy_revision_and_stopped_strategy_are_rejected() {
        let (binding, mut snapshot, cursor, event) = fixture();
        snapshot.instance.policy_revision += 1;
        assert_eq!(
            binding.preview(&snapshot, &cursor, &event),
            Err(GraphTargetError::StrategyStoppedOrChanged)
        );
        snapshot.instance.policy_revision -= 1;
        snapshot.instance.enabled = false;
        assert_eq!(
            binding.preview(&snapshot, &cursor, &event),
            Err(GraphTargetError::StrategyStoppedOrChanged)
        );
    }

    #[test]
    fn graph_session_time_and_sequence_mismatches_fail_closed() {
        let (binding, snapshot, cursor, mut event) = fixture();
        event.graph_sha256 = "b".repeat(64);
        assert_eq!(
            binding.preview(&snapshot, &cursor, &event),
            Err(GraphTargetError::PublicationMismatch)
        );
        event.graph_sha256 = binding.graph_sha256.clone();
        event.closed_ms += 1;
        assert_eq!(
            binding.preview(&snapshot, &cursor, &event),
            Err(GraphTargetError::PublicationMismatch)
        );
        event.closed_ms -= 1;
        event.sequence = 4;
        assert_eq!(
            binding.preview(&snapshot, &cursor, &event),
            Err(GraphTargetError::CursorInvalid(
                "GRAPH_PUBLICATION_SEQUENCE_GAP"
            ))
        );
    }

    #[test]
    fn hold_is_explicitly_not_a_flatten_and_bad_lots_fail_closed() {
        let (binding, mut snapshot, cursor, mut event) = fixture();
        snapshot.lots.push(owned_long());
        event.intent = GraphSignalIntent::Hold;
        let result = binding
            .preview(&snapshot, &cursor, &event)
            .unwrap()
            .unwrap();
        assert_eq!(result.target, TargetPosition::Hold);
        assert_eq!(result.plan, TargetPlan::NoChange);
        event.intent = GraphSignalIntent::SetTarget(GraphPosition::Flat);
        snapshot.lots[0].intent_id = None;
        assert_eq!(
            binding.preview(&snapshot, &cursor, &event),
            Err(GraphTargetError::Plan(
                TargetPlanError::MissingCloseReference("owned-lot".into())
            ))
        );
    }
}

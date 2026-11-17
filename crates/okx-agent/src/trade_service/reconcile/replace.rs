//! Replace an intent through the existing guarded execution path.
use super::*;

impl TradeService {
    pub(in crate::trade_service) async fn replace(
        &self,
        r: &Arc<AccountResources>,
        request: &TradeRequest,
        governance: Option<&CentralGovernanceSnapshot>,
    ) -> Result<Value, String> {
        let TradeRequest::Execute {
            idempotency_key,
            order,
            instrument,
            strategy_instance,
            side,
            expires_in_ms,
            ..
        } = request
        else {
            return Err("EXECUTE_REQUIRED".into());
        };
        let begun = Instant::now();
        let hash = digest(&(r.configuration.account_key.wire(), request))?;
        let key = idempotency_key.clone();
        if let Some(old) = r
            .execution
            .store
            .lookup_intent(
                deadline(),
                okx_execution::TradeIntentLookup::IdempotencyKey(key),
            )
            .await
            .map_err(err)?
        {
            if old.request_hash != hash {
                return Err("IDEMPOTENCY_CONFLICT".into());
            }
            return self.status_intent(r, &old.intent_id, None).await;
        }
        let reference = order
            .reference
            .as_ref()
            .ok_or("REPLACE_REFERENCE_REQUIRED")?;
        let target = self.find_intent(r, reference).await?;
        if target.instrument_id != *instrument
            || target.plan.strategy_instance != *strategy_instance
            || target.plan.position_side
                != if *side == PositionSide::Long {
                    "long"
                } else {
                    "short"
                }
        {
            return Err("REPLACE_SCOPE_MISMATCH".into());
        }
        let cancel_key = format!("replace_cancel_{}", &hash[..40]);
        let cancel = TradeRequest::Cancel {
            idempotency_key: cancel_key.clone(),
            reference: reference.clone(),
        };
        let _ = self.cancel(r, &cancel_key, reference, &cancel).await?;
        let after = self.find_intent(r, &target.intent_id).await?;
        if !matches!(after.state, IntentState::Cancelled | IntentState::Rejected) {
            return Ok(uncertain(&after.intent_id, "REPLACE_CANCEL_NOT_TERMINAL"));
        }
        // A timeout, not-found reply or incomplete venue order is UNKNOWN,
        // never evidence of a zero-fill cancel. No second POST on that path.
        let old_order = match r
            .execution
            .trade
            .order_by_client_id(&target.instrument_id, &target.client_order_id)
            .await
        {
            Ok(order) => order,
            Err(_) => {
                return Ok(uncertain(
                    &after.intent_id,
                    "REPLACE_ORDER_READBACK_UNKNOWN_NO_POST",
                ));
            }
        };
        verify_empty_canceled_order_for_replace(&after, &old_order)?;
        let remaining = expires_in_ms.saturating_sub(begun.elapsed().as_millis() as u64);
        if remaining == 0 {
            return Err("COMMAND_EXPIRED".into());
        }
        let mut replacement = request.clone();
        if let TradeRequest::Execute {
            operation,
            expires_in_ms,
            order,
            ..
        } = &mut replacement
        {
            *operation = TradeOperation::Open;
            *expires_in_ms = remaining;
            order.reference = None;
        }
        self.execute_bound(r, &replacement, governance, Some(hash))
            .await
    }
}

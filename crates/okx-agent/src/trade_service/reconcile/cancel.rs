//! Cancel an existing intent through its original durable operation and exchange admission.
use super::*;
use okx_api::CancelOrderRequest;

impl TradeService {
    pub(in crate::trade_service) async fn cancel(
        &self,
        r: &Arc<AccountResources>,
        key: &str,
        reference: &str,
        request: &TradeRequest,
    ) -> Result<Value, String> {
        let capacity = r
            .mutation_capacity
            .try_acquire()
            .map_err(|_| "ACCOUNT_MUTATION_BUSY")?;
        let i = self.find_intent(r, reference).await?;
        let hash = digest(&(r.configuration.account_key.wire(), request))?;
        let op = key.to_owned();
        let h = hash.clone();
        let fresh = r
            .execution
            .store
            .begin_operation(deadline(), op, h, "cancel".into())
            .await
            .map_err(err)?;
        if !fresh {
            // Status may legitimately clean up a now-flat protective order.
            // Do not hold this account's sole mutation permit across the
            // independent reconciliation and emergency safety path.
            drop(capacity);
            return self.status_intent(r, &i.intent_id, None).await;
        }
        if matches!(i.state, IntentState::Prepared | IntentState::Reserved) {
            let id = i.intent_id.clone();
            r.execution
                .store
                .reject_unsent(deadline(), id, "USER_CANCELLED_UNSENT".into())
                .await
                .map_err(err)?;
        } else if !i.state.terminal() {
            let timing = r
                .execution
                .clock
                .clock_evidence()
                .await
                .map_err(err)?
                .mutation_timing(5000)
                .map_err(err)?;
            let cancel = CancelOrderRequest {
                instrument_id: i.instrument_id.clone(),
                client_order_id: i.client_order_id.clone(),
            };
            let rate = r.execution.trade.admit_cancel_order(&cancel).map_err(err)?;
            // Cancels only the ordinary exact entry. Exchange-native protection is never canceled by this action.
            match r
                .execution
                .trade
                .cancel_order_after_admission(&cancel, &timing, &rate)
                .await
            {
                Ok(v) if v.top_level_success() && v.data.len() == 1 && v.data[0].accepted() => {}
                _ => return Ok(uncertain(&i.intent_id, "CANCEL_OUTCOME_UNKNOWN")),
            }
        }
        let op = key.to_owned();
        let h = hash;
        let result = i.intent_id.clone();
        r.execution
            .store
            .operation_receipt(deadline(), op, h, "cancel".into(), result)
            .await
            .map_err(err)?;
        // Cancel ACK is not a terminal financial proof. Reconciliation
        // must run after releasing the cancel mutation lease, so that its
        // independent protection cleanup/emergency owner can acquire it.
        drop(capacity);
        self.status_intent(r, &i.intent_id, None).await
    }
}

//! Close only the verified remaining lots belonging to the original intent.
use super::super::execute::GraphCloseContext;
use super::*;

impl TradeService {
    pub(in crate::trade_service) async fn close(
        &self,
        r: &Arc<AccountResources>,
        key: &str,
        reference: &str,
        expires: u64,
        governance: Option<&CentralGovernanceSnapshot>,
    ) -> Result<Value, String> {
        self.close_inner(r, key, reference, expires, governance, None)
            .await
    }

    /// Same original Close command builder, but with one immutable graph
    /// identity checked again at original finance PREPARE. This is internal
    /// only; background Graph consumer is not authorized to call it yet.
    pub(in crate::trade_service) async fn close_for_graph(
        &self,
        r: &Arc<AccountResources>,
        request: &TradeRequest,
        context: GraphCloseContext,
    ) -> Result<Value, String> {
        let TradeRequest::Close {
            idempotency_key,
            reference,
            expires_in_ms,
        } = request
        else {
            return Err("GRAPH_CLOSE_REQUEST_REQUIRED".into());
        };
        if context.original_reference != *reference {
            return Err("GRAPH_CLOSE_REFERENCE_MISMATCH".into());
        }
        self.close_inner(
            r,
            idempotency_key,
            reference,
            *expires_in_ms,
            None,
            Some(context),
        )
        .await
    }

    async fn close_inner(
        &self,
        r: &Arc<AccountResources>,
        key: &str,
        reference: &str,
        expires: u64,
        governance: Option<&CentralGovernanceSnapshot>,
        graph: Option<GraphCloseContext>,
    ) -> Result<Value, String> {
        let public = TradeRequest::Close {
            idempotency_key: key.into(),
            reference: reference.into(),
            expires_in_ms: expires,
        };
        let bound_hash = digest(&(r.configuration.account_key.wire(), &public))?;
        let k = key.to_owned();
        if let Some(prior) = r
            .execution
            .store
            .lookup_intent(
                deadline(),
                crate::execution_store::IntentLookup::new(
                    okx_execution::TradeIntentLookup::IdempotencyKey(k),
                )
                .map_err(err)?,
            )
            .await
            .map_err(err)?
        {
            if prior.request_hash != bound_hash {
                return Err("IDEMPOTENCY_CONFLICT".into());
            }
            return self.status_intent(r, &prior.intent_id, None).await;
        }
        let original = self.find_intent(r, reference).await?;
        let instrument = original.instrument_id.clone();
        let lots = r
            .execution
            .store
            .active_lots(deadline(), instrument)
            .await
            .map_err(err)?;
        let mine = lots
            .iter()
            .filter(|l| l.intent_id.as_deref() == Some(&original.intent_id))
            .collect::<Vec<_>>();
        let (side, quantity) = verified_close_lot_exposure(
            mine.iter()
                .map(|lot| (lot.side.as_str(), lot.remaining_qty)),
        )?;
        let command = TradeRequest::Execute {
            idempotency_key: key.into(),
            operation: TradeOperation::Close,
            strategy_instance: original.plan.strategy_instance,
            instrument: original.instrument_id,
            side,
            sizing: Sizing::Contracts {
                contracts: exact(quantity),
            },
            order: OrderConstraints {
                order_type: "ioc".into(),
                limit_price: None,
                reference: None,
                max_entry_usd: None,
                max_slippage_bps: 10,
            },
            protection: None,
            expires_in_ms: expires,
        };
        match graph {
            Some(context) => {
                self.execute_for_graph_close(r, &command, bound_hash, context)
                    .await
            }
            None => {
                self.execute_bound(r, &command, governance, Some(bound_hash))
                    .await
            }
        }
    }
}

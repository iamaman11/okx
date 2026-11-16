use super::super::*;
impl AccountStore {
    pub fn verify_protection(&mut self, p: &ProtectionReceipt) -> StoreResult<()> {
        Self::verify_protection_sql(&self.connection, None, p)
    }

    /// Read-only proof of a signed, completely billed partial reducing IOC.
    /// No account-wide position assumption, reservation release, or test
    /// money mutation. The terminal venue quantity is compared to EVERY
    /// attributed finance row, with a separate exact signed bill per fill.
    /// A partial reduction must stay locally outstanding until final flat.
    pub fn prove_local_partial_reduce(
        &self,
        id: &str,
        order_id: &str,
        venue_qty: Decimal,
        venue_at: i64,
    ) -> StoreResult<(Decimal, u64, String)> {
        Self::prove_local_partial_reduce_sql(
            &self.connection,
            None,
            id,
            order_id,
            venue_qty,
            venue_at,
        )
    }
    pub fn protection(&self, id: &str) -> StoreResult<Option<ProtectionReceipt>> {
        Self::protection_sql(&self.connection, None, id)
    }
    pub(crate) fn protection_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
    ) -> StoreResult<Option<ProtectionReceipt>> {
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
        }
        let (sql, args) = match scope {
            None => (
                "SELECT algo_id,side,quantity,trigger_price,trigger_basis,verified_at,source_digest FROM protection_receipts WHERE intent_id=?1",
                vec![rusqlite::types::Value::from(id.to_owned())],
            ),
            Some((i, a)) => (
                "SELECT algo_id,side,quantity,trigger_price,trigger_basis,verified_at,source_digest FROM protection_receipts WHERE intent_id=?1 AND installation_id=?2 AND account_key=?3",
                vec![
                    id.to_owned().into(),
                    i.to_owned().into(),
                    a.to_owned().into(),
                ],
            ),
        };
        let raw = db
            .query_row(sql, rusqlite::params_from_iter(args), |r| {
                Ok((
                    ProtectionReceipt {
                        intent_id: id.into(),
                        algo_id: r.get(0)?,
                        side: r.get(1)?,
                        quantity: Decimal::ZERO,
                        trigger_price: Decimal::ZERO,
                        trigger_basis: r.get(4)?,
                        verified_at: r.get(5)?,
                        source_digest: r.get(6)?,
                    },
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            })
            .optional()?;
        raw.map(|(mut p, q, t)| {
            p.quantity = decimal(q)?;
            p.trigger_price = decimal(t)?;
            Ok(p)
        })
        .transpose()
    }
    pub fn record_order_readback(
        &mut self,
        id: &str,
        order_id: &str,
        state: IntentState,
        cumulative_fill: Decimal,
        now: i64,
    ) -> StoreResult<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::record_order_readback_sql(&tx, None, id, order_id, state, cumulative_fill, now)?;
        tx.commit()?;
        Ok(())
    }
    pub(crate) fn record_order_readback_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        id: &str,
        order_id: &str,
        state: IntentState,
        cumulative_fill: Decimal,
        now: i64,
    ) -> StoreResult<()> {
        let i = Self::find_intent_sql(tx, scope, "intent_id", id)?
            .ok_or(StoreError::Invalid("INTENT_MISSING"))?;
        if cumulative_fill < Decimal::ZERO
            || cumulative_fill > i.plan.quantity
            || order_id.is_empty()
        {
            return Err(StoreError::Invalid("ORDER_READBACK_INVALID"));
        }
        let changed=match scope {
            Some((installation,account))=>tx.execute("INSERT INTO orders VALUES(?1,?2,?3,?3,?4,?5,NULL,?6,?7) ON CONFLICT(installation_id,account_key,order_id) DO UPDATE SET filled_qty=excluded.filled_qty,state=excluded.state WHERE intent_id=excluded.intent_id AND client_order_id=excluded.client_order_id",params![installation,account,order_id,id,i.client_order_id,exact(cumulative_fill),state.as_str()])?,
            None=>tx.execute("INSERT INTO orders VALUES(?1,?1,?2,?3,NULL,?4,?5) ON CONFLICT(order_id) DO UPDATE SET filled_qty=excluded.filled_qty,state=excluded.state WHERE intent_id=excluded.intent_id AND client_order_id=excluded.client_order_id",params![order_id,id,i.client_order_id,exact(cumulative_fill),state.as_str()])?,
        };
        if changed != 1 {
            return Err(StoreError::Invalid("ORDER_READBACK_IDENTITY_CONFLICT"));
        }
        Self::record_outcome_sql_transaction(
            tx,
            scope,
            id,
            state,
            Some(order_id),
            "authenticated-order-readback",
            now,
        )
    }
    /// Resolve a durable protection stop only after the caller has independently
    /// verified fresh flat venue state, complete finance/central settlement and
    /// zero pending algos on this instrument. Require the SAME original central
    /// release receipt and zero SQL lots in the transaction that clears stop,
    /// intent incident and seals immutable recovery evidence.
    pub fn resolve_protection_incident_after_flat(
        &mut self,
        id: &str,
        digest: &str,
        now: i64,
    ) -> StoreResult<bool> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = Self::resolve_protection_incident_after_flat_sql(&tx, None, id, digest, now)?;
        tx.commit()?;
        Ok(result)
    }
    pub fn protection_incident(&mut self, id: &str, reason: &str, now: i64) -> StoreResult<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::protection_incident_sql(&tx, None, id, reason, now)?;
        tx.commit()?;
        Ok(())
    }
}

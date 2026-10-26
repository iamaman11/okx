use super::super::*;
impl AccountStore {
    /// Durable address of a central reservation attempt, committed before
    /// any WSS RESERVE. A crash or lost ACK cannot orphan the Worker grant.
    pub fn mark_reserve_attempt(&mut self, id: &str, key: &str, now: i64) -> StoreResult<()> {
        if key.is_empty() || key.len() > 512 {
            return Err(StoreError::Invalid("INVALID_RESERVE_IDENTITY"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (state, hash, begin): (String, String, Option<i64>) = tx.query_row(
            "SELECT state,request_hash,begin_post_at FROM intents WHERE intent_id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        if state != "PREPARED" || begin.is_some() {
            return Err(StoreError::Invalid("INVALID_RESERVE_ATTEMPT_STATE"));
        }
        let name = format!("risk-reserve:{id}");
        let previous: Option<(String, String, String)> = tx
            .query_row(
                "SELECT kind,request_hash,result_ref FROM operation_receipts WHERE operation_id=?1",
                [&name],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        if let Some((kind, prior_hash, prior_key)) = previous {
            if kind != "RISK_RESERVE_ATTEMPT" || prior_hash != hash || prior_key != key {
                return Err(StoreError::Invalid("RESERVE_ATTEMPT_IDEMPOTENCY_CONFLICT"));
            }
        } else {
            tx.execute(
                "INSERT INTO operation_receipts VALUES(?1,'RISK_RESERVE_ATTEMPT',?2,?3,?4)",
                params![name, hash, key, now],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// An explicit central DENIED plus an authenticated address-specific
    /// NOT_FOUND readback proves this attempt minted no reservation. Both
    /// the original reason and its no-risk marker commit atomically.
    pub fn reject_denied_reserve(&mut self, id: &str, reason: &str, now: i64) -> StoreResult<()> {
        self.reject_unsent_inner(id, reason, now, true).map(|_| ())
    }

    pub fn declined_reserve(&self, id: &str) -> StoreResult<bool> {
        Ok(self.connection.query_row(
            "SELECT EXISTS(
               SELECT 1 FROM intents i
               JOIN operation_receipts o ON o.operation_id='risk-reserve:'||i.intent_id
               JOIN operation_receipts t ON t.operation_id='terminal:'||i.intent_id
               WHERE i.intent_id=?1 AND i.state='REJECTED' AND i.begin_post_at IS NULL
                 AND o.kind='RISK_RESERVE_DENIED'
                 AND o.request_hash=i.request_hash
                 AND t.kind='REJECTED_UNSENT' AND t.request_hash=i.request_hash)",
            [id],
            |r| r.get(0),
        )?)
    }

    pub fn reconcilable_intents(&self, limit: usize) -> StoreResult<Vec<StoredIntent>> {
        let ids = {
            let mut q=self.connection.prepare(
              "SELECT intent_id FROM (
                SELECT intent_id,CASE WHEN state='UNSENT_PENDING_CENTRAL' THEN 0 ELSE 2 END AS priority
                FROM risk_receipts
                WHERE state IN('HELD','POSTED','UNKNOWN','OPEN','TERMINAL_PENDING_CENTRAL','UNSENT_PENDING_CENTRAL')
                UNION ALL
                SELECT i.intent_id,1 AS priority FROM intents i
                JOIN operation_receipts o ON o.operation_id='risk-reserve:'||i.intent_id
                WHERE o.kind='RISK_RESERVE_ATTEMPT' AND o.request_hash=i.request_hash
                  AND i.state IN('PREPARED','REJECTED')
                  AND NOT EXISTS(SELECT 1 FROM risk_receipts r WHERE r.intent_id=i.intent_id)
                  AND NOT EXISTS(SELECT 1 FROM operation_receipts ack
                    WHERE ack.operation_id='central-release:'||o.result_ref
                      AND ack.kind='CENTRAL_RELEASE')
               ) GROUP BY intent_id ORDER BY MIN(priority),intent_id LIMIT ?1"
            )?;
            q.query_map([limit.min(1000) as i64], |r| r.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        ids.iter()
            .map(|id| {
                self.intent(id)?
                    .ok_or(StoreError::Invalid("INTENT_MISSING"))
            })
            .collect()
    }
    pub fn acknowledge_central_release(&mut self, key: &str, evidence: &str) -> StoreResult<()> {
        if evidence.is_empty() {
            return Err(StoreError::Invalid("CENTRAL_RELEASE_EVIDENCE_REQUIRED"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let hash: String = tx.query_row(
            "SELECT receipt_hash FROM risk_receipts WHERE reservation_key=?1
             UNION ALL SELECT request_hash FROM operation_receipts
               WHERE kind='RISK_RESERVE_ATTEMPT' AND result_ref=?1
             LIMIT 1",
            [key],
            |r| r.get(0),
        )?;
        tx.execute(
            "UPDATE risk_receipts SET state='RELEASED' WHERE reservation_key=?1",
            [key],
        )?;
        tx.execute("INSERT INTO operation_receipts VALUES(?1,'CENTRAL_RELEASE',?2,?3,?4) ON CONFLICT(operation_id) DO NOTHING",params![format!("central-release:{key}"),hash,evidence,wall_time_ms()])?;
        tx.commit()?;
        Ok(())
    }
    pub fn reject_unsent(&mut self, id: &str, reason: &str, now: i64) -> StoreResult<bool> {
        self.reject_unsent_inner(id, reason, now, false)
    }

    fn reject_unsent_inner(
        &mut self,
        id: &str,
        reason: &str,
        now: i64,
        proven_not_reserved: bool,
    ) -> StoreResult<bool> {
        if reason.is_empty() {
            return Err(StoreError::Invalid("UNSENT_REASON_REQUIRED"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (state, begin, hash): (String, Option<i64>, String) = tx.query_row(
            "SELECT state,begin_post_at,request_hash FROM intents WHERE intent_id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        if begin.is_some() {
            return Err(StoreError::Invalid("POST_OUTCOME_REQUIRES_RECONCILIATION"));
        }
        let first = state != "REJECTED";
        if first {
            if !matches!(state.as_str(), "PREPARED" | "RESERVED") {
                return Err(StoreError::Invalid("INVALID_INTENT_TRANSITION"));
            }
            tx.execute("UPDATE intents SET state='REJECTED',incident=?2,updated_at=?3,revision=revision+1 WHERE intent_id=?1",params![id,reason,now])?;
            tx.execute(
                "UPDATE risk_receipts SET state='UNSENT_PENDING_CENTRAL' WHERE intent_id=?1",
                [id],
            )?;
            tx.execute(
                "INSERT INTO operation_receipts VALUES(?1,'REJECTED_UNSENT',?2,?3,?4)",
                params![format!("terminal:{id}"), hash, reason, now],
            )?;
        }
        if proven_not_reserved {
            let original: Option<(String, String)> = tx
                .query_row(
                    "SELECT kind,request_hash FROM operation_receipts WHERE operation_id=?1",
                    [format!("terminal:{id}")],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            if original
                .as_ref()
                .is_none_or(|(kind, request)| kind != "REJECTED_UNSENT" || request != &hash)
            {
                return Err(StoreError::Invalid("UNSENT_TERMINAL_RECEIPT_MISSING"));
            }
            let operation = format!("risk-reserve:{id}");
            let changed = tx.execute(
                "UPDATE operation_receipts SET kind='RISK_RESERVE_DENIED'
                 WHERE operation_id=?1
                   AND kind IN('RISK_RESERVE_ATTEMPT','RISK_RESERVE_DENIED')",
                [&operation],
            )?;
            if changed != 1 {
                return Err(StoreError::Invalid("RESERVE_ATTEMPT_MARKER_MISSING"));
            }
        }
        tx.commit()?;
        Ok(first)
    }
    /// A no-send release requires the existing durable rejection receipt,
    /// matching the original request, with no committed post claim.
    /// A gateway timeout or venue rejection can never produce this witness.
    pub fn unsent_release_receipt(&self, id: &str) -> StoreResult<Option<UnsentSqlReceipt>> {
        self.connection
            .query_row(
                "SELECT COALESCE(r.reservation_key,a.result_ref),
                        COALESCE(r.account_key,?2),
                        i.intent_id,i.plan_hash,i.request_hash,i.incident
                 FROM intents i
                 LEFT JOIN risk_receipts r ON r.intent_id=i.intent_id
                 LEFT JOIN operation_receipts a ON a.operation_id='risk-reserve:'||i.intent_id
                    AND a.kind='RISK_RESERVE_ATTEMPT' AND a.request_hash=i.request_hash
                 JOIN operation_receipts o ON o.operation_id='terminal:'||i.intent_id
                 WHERE i.intent_id=?1 AND i.state='REJECTED'
                   AND i.begin_post_at IS NULL AND i.exchange_order_id IS NULL
                   AND (r.state IN('UNSENT_PENDING_CENTRAL','RELEASED') OR
                        (r.reservation_key IS NULL AND a.result_ref IS NOT NULL))
                   AND o.kind='REJECTED_UNSENT'
                   AND o.request_hash=i.request_hash AND o.result_ref=i.incident",
                params![id, self.meta.account_key],
                |r| {
                    Ok(UnsentSqlReceipt {
                        reservation_key: r.get(0)?,
                        account_key: r.get(1)?,
                        intent_id: r.get(2)?,
                        plan_hash: r.get(3)?,
                        request_hash: r.get(4)?,
                        reason: r.get(5)?,
                    })
                },
            )
            .optional()
            .map_err(StoreError::from)
    }
    pub fn central_receipt(&self, id: &str) -> StoreResult<Option<CentralReceipt>> {
        let row=self.connection.query_row("SELECT reservation_key,account_key,installation_id,instance_id,plan_hash,policy_revision,receipt_hash,requested_loss_usd,conversion_digest FROM risk_receipts WHERE intent_id=?1",[id],|r|Ok((CentralReceipt{reservation_key:r.get(0)?,account_key:r.get(1)?,installation_id:r.get(2)?,instance_id:r.get(3)?,intent_id:id.into(),plan_hash:r.get(4)?,policy_revision:r.get(5)?,receipt_hash:r.get(6)?,requested_loss_usd:Decimal::ZERO,conversion_digest:r.get(8)?},r.get::<_,String>(7)?))).optional()?;
        row.map(|(mut receipt, amount)| {
            receipt.requested_loss_usd = decimal(amount)?;
            Ok(receipt)
        })
        .transpose()
    }
    pub fn by_exchange_order_id(&self, key: &str) -> StoreResult<Option<StoredIntent>> {
        self.find_intent("exchange_order_id", key)
    }
    pub fn prepare(
        &mut self,
        decision: &Decision,
        intent: &NewIntent,
    ) -> StoreResult<StoredIntent> {
        if intent.plan.contract_value <= Decimal::ZERO
            || intent.plan.settlement_currency.is_empty()
            || !matches!(intent.plan.position_side.as_str(), "long" | "short")
        {
            return Err(StoreError::Invalid("INVALID_CONTRACT_MECHANICS"));
        }
        if let Some(existing) = self.by_idempotency(&intent.idempotency_key)? {
            if existing.request_hash != intent.request_hash {
                return Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"));
            }
            return Ok(existing);
        }
        if intent.deadline <= decision.decision_at
            || intent.policy_revision != decision.policy_revision
        {
            return Err(StoreError::Invalid("INVALID_INTENT_DEADLINE_OR_POLICY"));
        }
        if intent.plan.quantity <= Decimal::ZERO
            || intent.plan.limit_price <= Decimal::ZERO
            || intent.plan.planned_loss_usd < Decimal::ZERO
            || !matches!(intent.plan.side.as_str(), "buy" | "sell")
            || intent.plan.stop_price.is_some_and(|p| p <= Decimal::ZERO)
        {
            return Err(StoreError::Invalid("INVALID_INTENT_PLAN"));
        }
        let definition = self.strategy_definition(&intent.plan.strategy_instance)?;
        if definition.definition_id != intent.plan.strategy_definition_id
            || definition.version != intent.plan.strategy_definition_version
            || definition.parameters_digest != intent.plan.strategy_parameters_digest
        {
            return Err(StoreError::Invalid("STRATEGY_VERSION_CHANGED"));
        }
        if decision
            .instance_id
            .as_ref()
            .is_some_and(|id| id != &intent.plan.strategy_instance)
        {
            return Err(StoreError::Invalid("STRATEGY_OWNER_MISMATCH"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        // Manual commands insert their decision with the intent. Autonomous
        // strategies instead reuse the exact pending SQL decision already
        // durably claimed before planning; no second decision is permitted.
        let claimed_strategy = decision.decision_id.starts_with("strategy:");
        if claimed_strategy {
            let prior: Option<(Option<String>, String, i64)> = tx
                .query_row(
                    "SELECT instance_id,input_digest,policy_revision FROM decisions WHERE decision_id=?1",
                    [&decision.decision_id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            let (owner, digest, revision) =
                prior.ok_or(StoreError::Invalid("STRATEGY_CLAIM_MISSING"))?;
            if owner != decision.instance_id
                || digest != decision.input_digest
                || revision != decision.policy_revision
            {
                return Err(StoreError::Invalid("STRATEGY_CLAIM_MISMATCH"));
            }
            let already_bound: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM intents WHERE decision_id=?1)",
                [&decision.decision_id],
                |r| r.get(0),
            )?;
            if already_bound {
                return Err(StoreError::Invalid("STRATEGY_DECISION_ALREADY_BOUND"));
            }
        } else {
            tx.execute(
                "INSERT INTO decisions VALUES(?1,?2,?3,?4,?5,?6)",
                params![
                    decision.decision_id,
                    decision.instance_id,
                    decision.decision_at,
                    decision.input_digest,
                    decision.policy_revision,
                    decision.code_version
                ],
            )?;
        }
        tx.execute("INSERT INTO intents(intent_id,idempotency_key,request_hash,decision_id,state,revision,client_order_id,deadline,plan_hash,policy_revision,risk_increase,instrument_id,created_at,updated_at) VALUES(?1,?2,?3,?4,'PREPARED',0,?5,?6,?7,?8,?9,?10,?11,?11)",params![intent.intent_id,intent.idempotency_key,intent.request_hash,decision.decision_id,intent.client_order_id,intent.deadline,intent.plan_hash,intent.policy_revision,intent.risk_increase,intent.instrument_id,decision.decision_at])?;
        tx.execute(
            "INSERT INTO intent_plans VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
            params![
                intent.intent_id,
                intent.plan.operation,
                intent.plan.side,
                exact(intent.plan.quantity),
                exact(intent.plan.limit_price),
                intent.plan.stop_price.map(exact),
                intent.plan.strategy_instance,
                exact(intent.plan.planned_loss_usd),
                intent.plan.source_generation,
                intent.plan.position_side,
                intent.plan.private_source_generation,
                exact(intent.plan.contract_value),
                intent.plan.inverse,
                intent.plan.settlement_currency,
                intent.plan.strategy_definition_id, intent.plan.strategy_definition_version,intent.plan.strategy_parameters_digest
            ],
        )?;
        if claimed_strategy {
            // Binding and bar completion are one transaction. A crash before
            // commit leaves a retriable claim; after commit the intent itself
            // is the unique persisted decision outcome.
            tx.execute(
                "UPDATE strategy_instances SET last_decision_key=?2 WHERE instance_id=?1",
                params![intent.plan.strategy_instance, intent.idempotency_key],
            )?;
        }
        tx.commit()?;
        self.intent(&intent.intent_id)?
            .ok_or(StoreError::Invalid("INTENT_MISSING"))
    }
    pub fn intent(&self, id: &str) -> StoreResult<Option<StoredIntent>> {
        self.find_intent("intent_id", id)
    }
    pub fn by_idempotency(&self, key: &str) -> StoreResult<Option<StoredIntent>> {
        self.find_intent("idempotency_key", key)
    }
    pub fn by_client_order_id(&self, key: &str) -> StoreResult<Option<StoredIntent>> {
        self.find_intent("client_order_id", key)
    }
    pub fn operation(&self, id: &str) -> StoreResult<Option<OperationReceipt>> {
        Ok(self.connection.query_row("SELECT kind,request_hash,result_ref,committed_at FROM operation_receipts WHERE operation_id=?1",[id],|r|Ok(OperationReceipt{operation_id:id.into(),kind:r.get(0)?,request_hash:r.get(1)?,result_ref:r.get(2)?,committed_at:r.get(3)?})).optional()?)
    }
    // Durable marker is committed before any cancel/amend exchange mutation.
    pub fn begin_operation(
        &mut self,
        id: &str,
        hash: &str,
        kind: &str,
        now: i64,
    ) -> StoreResult<bool> {
        if let Some(old) = self.operation(id)? {
            if old.request_hash != hash || old.kind != kind {
                return Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"));
            }
            return Ok(false);
        }
        self.connection.execute(
            "INSERT INTO operation_receipts VALUES(?1,?2,?3,'POSTING',?4)",
            params![id, kind, hash, now],
        )?;
        Ok(true)
    }
    pub fn operation_receipt(
        &mut self,
        id: &str,
        hash: &str,
        kind: &str,
        result: &str,
        now: i64,
    ) -> StoreResult<OperationReceipt> {
        if let Some(old) = self.operation(id)? {
            if old.request_hash != hash || old.kind != kind {
                return Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"));
            }
            if old.result_ref != "POSTING" && old.result_ref != result {
                return Err(StoreError::Invalid("OPERATION_RESULT_CONFLICT"));
            }
        }
        self.connection.execute("INSERT INTO operation_receipts VALUES(?1,?2,?3,?4,?5) ON CONFLICT(operation_id) DO UPDATE SET result_ref=excluded.result_ref,committed_at=excluded.committed_at",params![id,kind,hash,result,now])?;
        self.operation(id)?
            .ok_or(StoreError::Invalid("OPERATION_MISSING"))
    }
    pub(super) fn find_intent(&self, column: &str, key: &str) -> StoreResult<Option<StoredIntent>> {
        let sql = format!(
            "SELECT intent_id,idempotency_key,request_hash,client_order_id,state,revision,deadline,plan_hash,policy_revision,risk_increase,instrument_id,exchange_order_id,incident FROM intents WHERE {column}=?1"
        );
        let raw = self
            .connection
            .query_row(&sql, [key], |r| {
                Ok((
                    StoredIntent {
                        plan: IntentPlan::default(),
                        intent_id: r.get(0)?,
                        idempotency_key: r.get(1)?,
                        request_hash: r.get(2)?,
                        client_order_id: r.get(3)?,
                        state: IntentState::Prepared,
                        revision: r.get(5)?,
                        deadline: r.get(6)?,
                        plan_hash: r.get(7)?,
                        policy_revision: r.get(8)?,
                        risk_increase: r.get(9)?,
                        instrument_id: r.get(10)?,
                        exchange_order_id: r.get(11)?,
                        incident: r.get(12)?,
                    },
                    r.get::<_, String>(4)?,
                ))
            })
            .optional()?;
        raw.map(|(mut i, s)| {
            i.state = IntentState::parse(&s)?;
            i.plan = self.plan(&i.intent_id)?;
            Ok(i)
        })
        .transpose()
    }
    pub(super) fn plan(&self, id: &str) -> StoreResult<IntentPlan> {
        let (mut p,q,price,stop,loss,cv)=self.connection.query_row("SELECT operation,side,quantity,limit_price,stop_price,strategy_instance,planned_loss_usd,source_generation,position_side,private_source_generation,contract_value,inverse,settlement_currency,strategy_definition_id,strategy_definition_version,strategy_parameters_digest FROM intent_plans WHERE intent_id=?1",[id],|r|Ok((IntentPlan{operation:r.get(0)?,side:r.get(1)?,strategy_instance:r.get(5)?,source_generation:r.get(7)?,position_side:r.get(8)?,private_source_generation:r.get(9)?,inverse:r.get(11)?,settlement_currency:r.get(12)?,strategy_definition_id:r.get(13)?,strategy_definition_version:r.get(14)?,strategy_parameters_digest:r.get(15)?,..Default::default()},r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,Option<String>>(4)?,r.get::<_,String>(6)?,r.get::<_,String>(10)?)))?;
        p.contract_value = decimal(cv)?;
        p.quantity = decimal(q)?;
        p.limit_price = decimal(price)?;
        p.stop_price = stop.map(decimal).transpose()?;
        p.planned_loss_usd = decimal(loss)?;
        Ok(p)
    }
    pub fn reserve(&mut self, receipt: &CentralReceipt, now: i64) -> StoreResult<()> {
        let i = self
            .intent(&receipt.intent_id)?
            .ok_or(StoreError::Invalid("INTENT_MISSING"))?;
        if receipt.account_key != self.meta.account_key
            || receipt.installation_id != self.meta.installation_id
            || receipt.plan_hash != i.plan_hash
            || receipt.policy_revision != i.policy_revision
            || receipt.receipt_hash.is_empty()
            || receipt.requested_loss_usd < Decimal::ZERO
            || receipt.conversion_digest.is_empty()
        {
            return Err(StoreError::Invalid("CENTRAL_RECEIPT_MISMATCH"));
        }
        if i.state == IntentState::Reserved {
            let saved: String = self.connection.query_row(
                "SELECT receipt_hash FROM risk_receipts WHERE reservation_key=?1",
                [&receipt.reservation_key],
                |r| r.get(0),
            )?;
            return if saved == receipt.receipt_hash {
                Ok(())
            } else {
                Err(StoreError::Invalid("CENTRAL_RECEIPT_CONFLICT"))
            };
        }
        if i.state != IntentState::Prepared {
            return Err(StoreError::Invalid("INVALID_INTENT_TRANSITION"));
        }
        let owner: Option<String> = self.connection.query_row(
            "SELECT instance_id FROM decisions JOIN intents USING(decision_id) WHERE intent_id=?1",
            [&i.intent_id],
            |r| r.get(0),
        )?;
        if owner != receipt.instance_id {
            return Err(StoreError::Invalid("CENTRAL_INSTANCE_MISMATCH"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO risk_receipts VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,'HELD')",
            params![
                receipt.reservation_key,
                receipt.intent_id,
                receipt.account_key,
                receipt.installation_id,
                receipt.instance_id,
                receipt.plan_hash,
                receipt.policy_revision,
                receipt.receipt_hash,
                exact(receipt.requested_loss_usd),
                receipt.conversion_digest
            ],
        )?;
        tx.execute("UPDATE intents SET state='RESERVED',revision=revision+1,updated_at=?2 WHERE intent_id=?1 AND state='PREPARED'",params![i.intent_id,now])?;
        tx.commit()?;
        Ok(())
    }
    // Returns true exactly once. The caller must never POST when this returns false/error.
    pub fn begin_post(&mut self, id: &str, policy_revision: i64, now: i64) -> StoreResult<bool> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (state, deadline, revision, increase): (String, i64, i64, bool) = tx.query_row(
            "SELECT state,deadline,policy_revision,risk_increase FROM intents WHERE intent_id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?;
        if state != "RESERVED" {
            return Ok(false);
        }
        if now >= deadline {
            return Err(StoreError::Invalid("COMMAND_EXPIRED"));
        }
        let (current, paused): (i64, bool) = tx.query_row(
            "SELECT revision,paused FROM account_policy WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if revision != policy_revision || current != revision {
            return Err(StoreError::Invalid("POLICY_CHANGED"));
        }
        if increase {
            let same:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM intent_plans p JOIN strategy_instances i ON i.instance_id=p.strategy_instance WHERE p.intent_id=?1 AND i.definition_id=p.strategy_definition_id AND i.definition_version=p.strategy_definition_version AND i.parameters_digest=p.strategy_parameters_digest AND (i.enabled=1 OR i.instance_id='manual'))",[id],|r|r.get(0))?;
            if !same {
                return Err(StoreError::Invalid("STRATEGY_VERSION_CHANGED"));
            }
            let stops: i64 = tx.query_row(
                "SELECT count(*) FROM risk_stops WHERE resolution IS NULL",
                [],
                |r| r.get(0),
            )?;
            if paused || stops > 0 {
                return Err(StoreError::Invalid("RISK_INCREASE_STOPPED"));
            }
        }
        if increase {
            let recovery: bool =
                tx.query_row("SELECT requires_reconcile FROM storage_recovery", [], |r| {
                    r.get(0)
                })?;
            if recovery {
                return Err(StoreError::Invalid("STORAGE_RECOVERY_INCOMPLETE"));
            }
        }
        if increase {
            let unknown: i64 = tx.query_row(
                "SELECT count(*) FROM intents WHERE state IN('POSTING','UNKNOWN')",
                [],
                |r| r.get(0),
            )?;
            if unknown > 0 {
                return Err(StoreError::Invalid("ACCOUNT_UNKNOWN_EXPOSURE"));
            }
        }
        tx.execute("UPDATE intents SET state='POSTING',begin_post_at=?2,updated_at=?2,revision=revision+1 WHERE intent_id=?1 AND state='RESERVED' AND begin_post_at IS NULL",params![id,now])?;
        tx.execute(
            "UPDATE risk_receipts SET state='POSTED' WHERE intent_id=?1",
            [id],
        )?;
        tx.commit()?;
        Ok(true)
    }
    // Exchange readback is authoritative; a timeout or negative lookup is UNKNOWN.
    pub fn record_outcome(
        &mut self,
        id: &str,
        state: IntentState,
        exchange_order_id: Option<&str>,
        evidence: &str,
        now: i64,
    ) -> StoreResult<()> {
        let i = self
            .intent(id)?
            .ok_or(StoreError::Invalid("INTENT_MISSING"))?;
        let allowed = match i.state {
            IntentState::Posting | IntentState::Unknown => matches!(
                state,
                IntentState::Unknown
                    | IntentState::Acknowledged
                    | IntentState::PartiallyFilled
                    | IntentState::Filled
                    | IntentState::Cancelled
                    | IntentState::Rejected
            ),
            IntentState::Acknowledged => matches!(
                state,
                IntentState::Acknowledged
                    | IntentState::PartiallyFilled
                    | IntentState::Filled
                    | IntentState::Cancelled
            ),
            IntentState::PartiallyFilled => matches!(
                state,
                IntentState::PartiallyFilled | IntentState::Filled | IntentState::Cancelled
            ),
            _ => i.state == state,
        };
        if !allowed || evidence.is_empty() {
            return Err(StoreError::Invalid("INVALID_INTENT_TRANSITION"));
        }
        if i.exchange_order_id
            .as_deref()
            .is_some_and(|old| Some(old) != exchange_order_id)
        {
            return Err(StoreError::Invalid("EXCHANGE_ID_CONFLICT"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute("UPDATE intents SET state=?2,exchange_order_id=COALESCE(exchange_order_id,?3),updated_at=?4,revision=revision+1 WHERE intent_id=?1",params![id,state.as_str(),exchange_order_id,now])?;
        tx.execute(
            "UPDATE risk_receipts SET state=?2 WHERE intent_id=?1",
            params![
                id,
                if state == IntentState::Unknown {
                    "UNKNOWN"
                } else if i.risk_increase
                    && matches!(state, IntentState::Filled | IntentState::PartiallyFilled)
                {
                    "OPEN"
                } else if state.terminal() {
                    "TERMINAL_PENDING_CENTRAL"
                } else {
                    "OPEN"
                }
            ],
        )?;
        if state.terminal() {
            tx.execute("INSERT INTO operation_receipts VALUES(?1,'TERMINAL',?2,?3,?4) ON CONFLICT(operation_id) DO NOTHING",params![format!("terminal:{id}"),i.request_hash,evidence,now])?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn recover_posting(&mut self, now: i64) -> StoreResult<usize> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let n=tx.execute("UPDATE intents SET state='UNKNOWN',updated_at=?1,revision=revision+1 WHERE state='POSTING'",[now])?;
        tx.execute("UPDATE risk_receipts SET state='UNKNOWN' WHERE intent_id IN(SELECT intent_id FROM intents WHERE state='UNKNOWN')",[])?;
        tx.commit()?;
        Ok(n)
    }
    pub fn active_intents(&self, limit: usize) -> StoreResult<Vec<StoredIntent>> {
        let mut s=self.connection.prepare("SELECT intent_id FROM intents WHERE state IN('PREPARED','RESERVED','POSTING','UNKNOWN','ACKNOWLEDGED','PARTIALLY_FILLED') ORDER BY state,updated_at,intent_id LIMIT ?1")?;
        let ids = s
            .query_map([limit.min(1000) as i64], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.iter()
            .map(|id| {
                self.intent(id)?
                    .ok_or(StoreError::Invalid("INTENT_MISSING"))
            })
            .collect()
    }
    // Cache an already-authorized revision. This grants no new financial authority.
}

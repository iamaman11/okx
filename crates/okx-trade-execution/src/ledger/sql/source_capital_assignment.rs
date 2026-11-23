//! Authorized internal capital assignment: two conserved legs and one receipt.
use super::super::*;
impl AccountStore {
    pub(crate) fn commit_authorized_capital_assignment_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        request: &AuthorizedCapitalAssignment,
    ) -> StoreResult<OperationReceipt> {
        use rusqlite::{params_from_iter, types::Value};
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(tx, installation, account)?;
        }
        let (account_key,installation_id,initialized_at):(String,String,i64)=match scope {
            None=>tx.query_row("SELECT account_key,installation_id,initialized_at FROM account_meta WHERE singleton=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?,
            Some((installation,account))=>tx.query_row("SELECT account_key,installation_id,initialized_at FROM account_meta WHERE singleton=1 AND installation_id=?1 AND account_key=?2",params![installation,account],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?
        };
        let values = |mut original: Vec<Value>| {
            if let Some((installation, account)) = scope {
                original.push(installation.to_owned().into());
                original.push(account.to_owned().into());
            }
            original
        };
        if request.account_key != account_key
            || request.installation_id != installation_id
            || request.nav_source_ref.is_empty()
            || request.instance_id == "manual"
            || request.instance_id.is_empty()
            || request.operation_id.is_empty()
            || request.request_hash.is_empty()
            || request.authority_receipt.is_empty()
            || request.authority_revision <= 0
            || request.time < initialized_at
            || request.delta_usd == Decimal::ZERO
        {
            return Err(StoreError::Invalid("INVALID_CAPITAL_ASSIGNMENT"));
        }
        if let Some(old) = Self::operation_sql(tx, scope, &request.operation_id)? {
            if old.kind != "CAPITAL_ASSIGNMENT" || old.request_hash != request.request_hash {
                return Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"));
            }
            return Ok(old);
        }
        // Current principal totals are a projection of this ordered series.
        // Backdated new assignments would corrupt that projection and
        // retroactively change NAV; reject them instead of silently rewriting
        // prior allocations. Exact idempotent retries still return above.
        let latest_assignment = Self::latest_capital_assignment_time_sql(tx, scope)?;
        if latest_assignment.is_some_and(|last| request.time < last) {
            return Err(StoreError::Invalid("CAPITAL_ASSIGNMENT_OUT_OF_ORDER"));
        }
        let state = Self::capital_state_sql(tx, scope, request.time, latest_assignment)?;
        if request.nav_source_ref != state.nav_source_ref {
            return Err(StoreError::Invalid("CAPITAL_NAV_SOURCE_MISMATCH"));
        }
        let existing = *state
            .strategy_principal_usd
            .get(&request.instance_id)
            .unwrap_or(&Decimal::ZERO);
        let next = existing
            .checked_add(request.delta_usd)
            .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
        if next < Decimal::ZERO
            || (request.delta_usd > Decimal::ZERO
                && request.delta_usd > state.unattributed_residual_usd)
        {
            return Err(StoreError::Invalid("CAPITAL_ASSIGNMENT_EXCEEDS_AVAILABLE"));
        }
        if Self::instance_sql(tx, scope, &request.instance_id)?.is_none() {
            return Err(StoreError::Invalid("STRATEGY_INSTANCE_NOT_FOUND"));
        }
        let (policy_revision,): (i64,) = tx.query_row(
            match scope {None=>"SELECT revision FROM account_policy WHERE singleton=1",Some(_)=>"SELECT revision FROM account_policy WHERE singleton=1 AND installation_id=?1 AND account_key=?2"},
            params_from_iter(values(vec![])),
            |r| Ok((r.get(0)?,)),
        )?;
        if policy_revision != request.authority_revision {
            return Err(StoreError::Invalid("CAPITAL_AUTHORITY_REVISION_MISMATCH"));
        }
        let finance_rowid_before = Self::finance_rowid_sql(tx, scope)?;
        // Book two opposite legs under one economic event. Neither leg is
        // income or expense. SQL immutability protects the event after commit.
        for (leg, owner, delta) in [
            (
                format!("strategy:{}", request.instance_id),
                Some(request.instance_id.as_str()),
                request.delta_usd,
            ),
            (
                String::from("unattributed"),
                None,
                Decimal::ZERO
                    .checked_sub(request.delta_usd)
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
            ),
        ] {
            let entry_id = format!("capital:{}:{leg}", request.operation_id);
            tx.execute(
                match scope {None=>"INSERT INTO finance_entries(entry_id,economic_event_key,component,allocation_leg,amount,currency,instance_id,source_ref,time,reverses_entry_id) VALUES(?1,?2,'INTERNAL_CAPITAL_USD',?3,?4,'USD',?5,?6,?7,NULL)",Some(_)=>"INSERT INTO finance_entries(installation_id,account_key,entry_id,economic_event_key,component,allocation_leg,amount,currency,instance_id,source_ref,time,reverses_entry_id) VALUES(?8,?9,?1,?2,'INTERNAL_CAPITAL_USD',?3,?4,'USD',?5,?6,?7,NULL)"},
                params_from_iter(values(vec![entry_id.clone().into(),format!("capital:{}",request.operation_id).into(),leg.into(),exact(delta).into(),owner.map(str::to_owned).into(),request.authority_receipt.clone().into(),request.time.into()]))
            )?;
            tx.execute(
                match scope {None=>"INSERT INTO finance_source_links(entry_id,source_ref) VALUES(?1,?2)",Some(_)=>"INSERT INTO finance_source_links(installation_id,account_key,entry_id,source_ref) VALUES(?3,?4,?1,?2)"},
                params_from_iter(values(vec![entry_id.clone().into(),request.authority_receipt.clone().into()])),
            )?;
        }
        tx.execute(
            match scope {None=>"INSERT INTO instance_finance_totals(instance_id,currency,component,amount) VALUES(?1,'USD','INTERNAL_CAPITAL_USD',?2) ON CONFLICT(instance_id,currency,component) DO UPDATE SET amount=excluded.amount",Some(_)=>"INSERT INTO instance_finance_totals(installation_id,account_key,instance_id,currency,component,amount) VALUES(?3,?4,?1,'USD','INTERNAL_CAPITAL_USD',?2) ON CONFLICT(installation_id,account_key,instance_id,currency,component) DO UPDATE SET amount=excluded.amount"},
            params_from_iter(values(vec![request.instance_id.clone().into(),exact(next).into()]))
        )?;
        // A single account aggregate represents BOTH legs and remains zero.
        tx.execute(
            match scope {None=>"INSERT INTO finance_totals(currency,component,amount) VALUES('USD','INTERNAL_CAPITAL_USD','0') ON CONFLICT(currency,component) DO NOTHING",Some(_)=>"INSERT INTO finance_totals(installation_id,account_key,currency,component,amount) VALUES(?1,?2,'USD','INTERNAL_CAPITAL_USD','0') ON CONFLICT(installation_id,account_key,currency,component) DO NOTHING"},
            params_from_iter(values(vec![]))
        )?;
        let result = format!(
            "strategy:{}|principal_usd:{}|account_nav_as_of:{}",
            request.instance_id,
            exact(next),
            state.nav_source_as_of
        );
        tx.execute(
            match scope {
                None => "INSERT INTO operation_receipts VALUES(?1,'CAPITAL_ASSIGNMENT',?2,?3,?4)",
                Some(_) => {
                    "INSERT INTO operation_receipts VALUES(?5,?6,?1,'CAPITAL_ASSIGNMENT',?2,?3,?4)"
                }
            },
            params_from_iter(values(vec![
                request.operation_id.clone().into(),
                request.request_hash.clone().into(),
                result.into(),
                request.time.into(),
            ])),
        )?;
        Self::advance_finance_provenance_sql(tx, scope, finance_rowid_before)?;
        Self::operation_sql(tx, scope, &request.operation_id)?
            .ok_or(StoreError::Invalid("CAPITAL_ASSIGNMENT_RECEIPT_MISSING"))
    }
}

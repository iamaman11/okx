//! Typed governance producer; SQL owns source selection, replay and both capital legs.
use super::*;
#[derive(Debug, Clone)]
pub struct CapitalAssignmentCommand {
    pub account_key: String,
    pub operation_id: String,
    pub request_hash: String,
    pub instance_id: String,
    pub delta_usd: Decimal,
    pub authority_revision: i64,
    pub authority_receipt: String,
    pub time: i64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CapitalAssignmentResult {
    pub receipt: OperationReceipt,
    pub capital: Option<AccountCapitalState>,
    pub capital_coverage: String,
}
impl AccountStore {
    pub fn assign_capital(
        &mut self,
        input: &CapitalAssignmentCommand,
    ) -> StoreResult<CapitalAssignmentResult> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = Self::assign_capital_sql(&tx, None, input)?;
        tx.commit()?;
        *self.latest_capital_assignment_time.borrow_mut() = None;
        Ok(result)
    }
    pub(crate) fn assign_capital_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        input: &CapitalAssignmentCommand,
    ) -> StoreResult<CapitalAssignmentResult> {
        let (account, installation): (String, String) = match scope {
            None => tx.query_row(
                "SELECT account_key,installation_id FROM account_meta WHERE singleton=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?,
            Some((i, a)) => {
                Self::verify_source_account_sql(tx, i, a)?;
                tx.query_row("SELECT account_key,installation_id FROM account_meta WHERE installation_id=?1 AND account_key=?2 AND singleton=1",params![i,a],|r|Ok((r.get(0)?,r.get(1)?)))?
            }
        };
        if input.account_key != account {
            return Err(StoreError::Invalid("INVALID_CAPITAL_ASSIGNMENT"));
        }
        let capital = || {
            Self::capital_state_sql(
                tx,
                scope,
                input.time,
                Self::latest_capital_assignment_time_sql(tx, scope)?,
            )
        };
        if let Some(prior) = Self::operation_sql(tx, scope, &input.operation_id)? {
            if prior.kind != "CAPITAL_ASSIGNMENT" || prior.request_hash != input.request_hash {
                return Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"));
            }
            let state = capital().ok();
            let coverage = state
                .as_ref()
                .map_or("PARTIAL", |v| v.coverage.as_str())
                .to_owned();
            return Ok(CapitalAssignmentResult {
                receipt: prior,
                capital: state,
                capital_coverage: coverage,
            });
        }
        let source = capital()?.nav_source_ref;
        let receipt = Self::commit_authorized_capital_assignment_sql(
            tx,
            scope,
            &AuthorizedCapitalAssignment {
                account_key: account,
                installation_id: installation,
                nav_source_ref: source,
                operation_id: input.operation_id.clone(),
                request_hash: input.request_hash.clone(),
                instance_id: input.instance_id.clone(),
                delta_usd: input.delta_usd,
                authority_revision: input.authority_revision,
                authority_receipt: input.authority_receipt.clone(),
                time: input.time,
            },
        )?;
        let state = capital()?;
        Ok(CapitalAssignmentResult {
            receipt,
            capital_coverage: state.coverage.clone(),
            capital: Some(state),
        })
    }
}

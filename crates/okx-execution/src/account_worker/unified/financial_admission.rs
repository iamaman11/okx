//! Typed financial admission on the installation writer, with owner time.
use super::*;
use crate::{AccountStore, FinancialAdmissionRequest, StoreError, StoreResult};
use std::time::Instant;
impl UnifiedFinanceHandle {
    pub async fn check_financial_admission(
        &self,
        deadline: Instant,
        account: String,
        request: FinancialAdmissionRequest,
    ) -> StoreResult<Option<String>> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let behavior = if request.record_risk_state {
                rusqlite::TransactionBehavior::Immediate
            } else {
                rusqlite::TransactionBehavior::Deferred
            };
            let tx = db.transaction_with_behavior(behavior)?;
            if request.record_risk_state {
                require_verified_live_finance_authority(&tx, &installation)?;
            }
            let now = super::opening_ingress::owner_now_ms()?;
            let result = AccountStore::check_financial_admission_sql(
                &tx,
                Some((&installation, &account)),
                &request,
                now,
            )?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(result)
        })
        .await
    }
}

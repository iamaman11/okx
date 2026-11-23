//! Typed account authority read models on the sole installation SQL snapshot.
use super::UnifiedFinanceHandle;
use crate::{AccountPolicySnapshot, AccountStore, CentralReceipt, CentralReceiptView, StoreResult};
use std::time::Instant;
impl UnifiedFinanceHandle {
    /// Original connected roster, read from the installation owner.
    pub async fn connected_account_keys(&self, deadline: Instant) -> StoreResult<Vec<String>> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline),move|db|{
            Ok(db.prepare("SELECT account_key FROM account_meta WHERE installation_id=?1 ORDER BY account_key")?
                .query_map([installation],|r|r.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?)
        }).await
    }

    /// The campaign identity belongs to canonical SQL, not launch arguments.
    /// A fresh installation has no policy yet; multiple campaigns are ambiguous.
    pub async fn configured_campaign(&self, deadline: Instant) -> StoreResult<Option<String>> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline),move|db|{
            let campaigns=db.prepare("SELECT campaign_id FROM risk_meta WHERE installation_id=?1 ORDER BY campaign_id LIMIT 2")?
                .query_map([installation],|r|r.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?;
            if campaigns.len()>1{return Err(crate::StoreError::Invalid("FINANCE_CAMPAIGN_AMBIGUOUS"));}
            Ok(campaigns.into_iter().next())
        }).await
    }

    /// Financial projection and unresolved stops from one read-only snapshot.
    /// This explains admission; it never creates a risk or venue grant.
    pub async fn financial_readiness(
        &self,
        account: String,
        instance: Option<String>,
        deadline: Instant,
    ) -> StoreResult<(crate::FinancialRisk, usize)> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let risk = AccountStore::financial_risk_sql(&tx, Some((&installation, &account)),
                super::opening_ingress::owner_now_ms()?, instance.as_deref())?;
            let stops: i64 = tx.query_row(
                "SELECT COUNT(*) FROM risk_stops WHERE installation_id=?1 AND account_key=?2 AND resolution IS NULL",
                rusqlite::params![installation,account], |r|r.get(0))?;
            tx.commit()?;
            Ok((risk, usize::try_from(stops).map_err(|_|crate::StoreError::Invalid("FINANCIAL_STOP_COUNT_INVALID"))?))
        }).await
    }

    pub async fn account_policy(
        &self,
        account: String,
        deadline: Instant,
    ) -> StoreResult<AccountPolicySnapshot> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let result = AccountStore::account_policy_sql(&tx, Some((&installation, &account)))?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }
    pub async fn reservation_receipt(
        &self,
        account: String,
        intent: String,
        view: CentralReceiptView,
        deadline: Instant,
    ) -> StoreResult<Option<CentralReceipt>> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let result = AccountStore::reservation_receipt_sql(
                &tx,
                Some((&installation, &account)),
                &intent,
                view,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }
}

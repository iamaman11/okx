//! Account policy and reservation observations, not financial or venue grants.
use super::*;
use crate::AccountStoreHandle;
use std::time::Instant;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountPolicySnapshot {
    pub revision: i64,
    pub paused: bool,
    pub campaign_id: String,
    pub campaign_limit: Decimal,
    pub campaign_loss: Decimal,
}
impl From<(i64, bool, String, Decimal, Decimal)> for AccountPolicySnapshot {
    fn from(v: (i64, bool, String, Decimal, Decimal)) -> Self {
        Self {
            revision: v.0,
            paused: v.1,
            campaign_id: v.2,
            campaign_limit: v.3,
            campaign_loss: v.4,
        }
    }
}
impl AccountPolicySnapshot {
    pub(crate) fn legacy_tuple(self) -> (i64, bool, String, Decimal, Decimal) {
        (
            self.revision,
            self.paused,
            self.campaign_id,
            self.campaign_limit,
            self.campaign_loss,
        )
    }
}
#[derive(Debug, Clone, Copy)]
pub enum CentralReceiptView {
    Retained,
    ActiveExposure,
}
impl AccountStore {
    pub(crate) fn receipt_covers_exposure(state: &str) -> bool {
        matches!(
            state,
            "HELD" | "POSTED" | "UNKNOWN" | "OPEN" | "TERMINAL_PENDING_CENTRAL"
        )
    }

    pub fn account_policy(&self) -> StoreResult<AccountPolicySnapshot> {
        Self::account_policy_sql(&self.connection, None)
    }
    pub(crate) fn account_policy_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
    ) -> StoreResult<AccountPolicySnapshot> {
        if let Some((i, a)) = scope {
            Self::verify_source_account_sql(db, i, a)?;
        }
        let (revision,paused,campaign_id,limit,loss):(i64,bool,String,String,String)=match scope {
            None=>db.query_row("SELECT revision,paused,campaign_id,campaign_limit,campaign_loss FROM account_policy",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?,
            Some((i,a))=>db.query_row("SELECT revision,paused,campaign_id,campaign_limit,campaign_loss FROM account_policy WHERE installation_id=?1 AND account_key=?2 AND singleton=1",params![i,a],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?,
        };
        Ok(AccountPolicySnapshot {
            revision,
            paused,
            campaign_id,
            campaign_limit: decimal(limit)?,
            campaign_loss: decimal(loss)?,
        })
    }
    pub fn reservation_receipt(
        &self,
        id: &str,
        view: CentralReceiptView,
    ) -> StoreResult<Option<CentralReceipt>> {
        Self::reservation_receipt_sql(&self.connection, None, id, view)
    }
    pub(crate) fn reservation_receipt_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
        view: CentralReceiptView,
    ) -> StoreResult<Option<CentralReceipt>> {
        if let Some((i, a)) = scope {
            Self::verify_source_account_sql(db, i, a)?;
        }
        let sql = match scope {
            None => {
                "SELECT reservation_key,account_key,installation_id,instance_id,plan_hash,policy_revision,receipt_hash,requested_loss_usd,conversion_digest,state FROM risk_receipts WHERE intent_id=?1"
            }
            Some(_) => {
                "SELECT reservation_key,account_key,installation_id,instance_id,plan_hash,policy_revision,receipt_hash,requested_loss_usd,conversion_digest,state FROM risk_receipts WHERE intent_id=?1 AND installation_id=?2 AND account_key=?3"
            }
        };
        let mut values = vec![rusqlite::types::Value::from(id.to_owned())];
        if let Some((i, a)) = scope {
            values.push(i.to_owned().into());
            values.push(a.to_owned().into());
        }
        let row = db
            .query_row(sql, rusqlite::params_from_iter(values), |r| {
                Ok((
                    CentralReceipt {
                        reservation_key: r.get(0)?,
                        account_key: r.get(1)?,
                        installation_id: r.get(2)?,
                        instance_id: r.get(3)?,
                        intent_id: id.into(),
                        plan_hash: r.get(4)?,
                        policy_revision: r.get(5)?,
                        receipt_hash: r.get(6)?,
                        requested_loss_usd: Decimal::ZERO,
                        conversion_digest: r.get(8)?,
                    },
                    r.get::<_, String>(7)?,
                    r.get::<_, String>(9)?,
                ))
            })
            .optional()?;
        row.map(|(mut receipt, amount, state)| {
            receipt.requested_loss_usd = decimal(amount)?;
            if matches!(view, CentralReceiptView::ActiveExposure)
                && !Self::receipt_covers_exposure(&state)
            {
                return Ok(None);
            }
            Ok(Some(receipt))
        })
        .transpose()
        .map(Option::flatten)
    }
}
impl AccountStoreHandle {
    pub async fn account_policy(&self, deadline: Instant) -> StoreResult<AccountPolicySnapshot> {
        self.call(deadline, |s| s.account_policy()).await
    }
    pub async fn reservation_receipt(
        &self,
        deadline: Instant,
        intent: String,
        view: CentralReceiptView,
    ) -> StoreResult<Option<CentralReceipt>> {
        self.call(deadline, move |s| s.reservation_receipt(&intent, view))
            .await
    }
}

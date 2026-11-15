//! Only a first successful combined fence COMMIT may create a Native send value.
use super::{
    UnifiedFinanceHandle, UnifiedFinanceWrite, UnifiedFinanceWriteReceipt,
    apply_original_unified_finance_transaction, require_verified_live_finance_authority,
};
use crate::{CommittedVenueSend, StoreError, StoreResult};
/// Not Clone/serde: dropping or losing this reply requires reconciliation.
pub(crate) struct NativePostCommit {
    pub claim: CommittedVenueSend,
    pub reservation: String,
    pub sequence: u64,
    pub existing_modelled_loss_usdt: String,
}
pub(crate) enum NativePostResult {
    Authorized(NativePostCommit),
    Denied { reason: String },
}
impl UnifiedFinanceHandle {
    pub(crate) async fn commit_native_post(
        &self,
        campaign: String,
        mut input: crate::risk::PostFenceInput,
    ) -> StoreResult<NativePostResult> {
        let installation = self.installation.clone();
        self.submit_before(Some(std::time::Instant::now()+std::time::Duration::from_secs(10)),move |db| {
            let tx=db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_verified_live_finance_authority(&tx,&installation)?;
            input.now_ms=super::opening_ingress::owner_now_ms()?;
            let reservation=input.reservation_key.clone();
            let plan=input.plan_sha256.clone();
            let decision=apply_original_unified_finance_transaction(&tx,&installation,UnifiedFinanceWrite::BeginPost {campaign:campaign.clone(),input})?;
            let UnifiedFinanceWriteReceipt::Risk(crate::risk::UnifiedRiskDecision::PostAuthorized {book_sequence,existing_modelled_loss_usdt,..})=decision else {
                // An ordinary denial may preserve observed loss high-water.
                if let UnifiedFinanceWriteReceipt::Risk(crate::risk::UnifiedRiskDecision::Denied {reason})=decision {
                    tx.commit().map_err(|_|StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY"))?;
                    return Ok(NativePostResult::Denied {reason});
                }
                return Err(StoreError::Invalid("NATIVE_POST_FENCE_RECEIPT_UNEXPECTED"));
            };
            let (account,intent):(String,String)=tx.query_row("SELECT account_key,intent_id FROM risk_entries WHERE installation_id=?1 AND campaign_id=?2 AND reservation_key=?3 AND plan_sha256=?4 AND state='POSTED'",rusqlite::params![installation,campaign,reservation,plan],|r|Ok((r.get(0)?,r.get(1)?)))?;
            tx.commit().map_err(|_|StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY"))?;
            // Mint AFTER COMMIT, in the owner job. A lost oneshot drops it;
            // no read API or replay can reconstruct the capability.
            Ok(NativePostResult::Authorized(NativePostCommit {claim:CommittedVenueSend::after_commit(installation,account,intent,plan),reservation,sequence:book_sequence,existing_modelled_loss_usdt}))
        }).await
    }
}

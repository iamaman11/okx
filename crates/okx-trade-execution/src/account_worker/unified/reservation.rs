//! Native durable reservation identity belongs to the local transaction,
//! independently of serialization/delivery of a transport HELD response.
use crate::{AccountStore, CentralReceipt, StoreError, StoreResult};
use sha2::{Digest, Sha256};
pub(crate) fn native_reservation_receipt_sql(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    campaign: &str,
    input: &crate::risk::ReserveInput,
) -> StoreResult<CentralReceipt> {
    use rusqlite::OptionalExtension;
    let account = format!("{}:real:{}", input.venue, input.uid);
    let intent = AccountStore::lookup_trade_intent_sql(
        tx,
        Some((installation, &account)),
        &crate::TradeIntentLookup::IntentId(input.intent_id.clone()),
    )?
    .ok_or(StoreError::Invalid("RESERVE_SOURCE_INTENT_MISSING"))?;
    if intent.plan_hash != input.plan_sha256
        || intent.policy_revision as u64 != input.policy_revision
    {
        return Err(StoreError::Invalid(
            "NATIVE_RESERVE_SOURCE_IDENTITY_MISMATCH",
        ));
    }
    let owner:Option<String>=tx.query_row("SELECT d.instance_id FROM intents i JOIN decisions d ON d.installation_id=i.installation_id AND d.account_key=i.account_key AND d.decision_id=i.decision_id WHERE i.installation_id=?1 AND i.account_key=?2 AND i.intent_id=?3",rusqlite::params![installation,account,input.intent_id],|r|r.get(0)).optional()?.flatten();
    let loss = input
        .proposal
        .loss_usd
        .canonical()
        .map_err(|_| StoreError::Invalid("MONEY_OVERFLOW"))?;
    let reservation = input.reservation_key();
    // Stable typed domain preimage. No response sequence, wall-clock or
    // process-specific fields: exact retry/reopen retains the first identity.
    let preimage = serde_json::to_vec(&(
        "okx.native-reservation-receipt/v1",
        installation,
        campaign,
        &account,
        &input.intent_id,
        &reservation,
        &input.plan_sha256,
        input.policy_revision,
        &owner,
        &loss,
        &input.conversion_digest,
    ))
    .map_err(|_| StoreError::Invalid("NATIVE_RESERVE_RECEIPT_ENCODING"))?;
    Ok(CentralReceipt {
        reservation_key: reservation,
        account_key: account,
        installation_id: installation.into(),
        instance_id: owner,
        intent_id: input.intent_id.clone(),
        plan_hash: input.plan_sha256.clone(),
        policy_revision: i64::try_from(input.policy_revision)
            .map_err(|_| StoreError::Invalid("POLICY_CHANGED"))?,
        receipt_hash: format!("{:x}", Sha256::digest(preimage)),
        requested_loss_usd: loss
            .parse()
            .map_err(|_| StoreError::Invalid("MONEY_OVERFLOW"))?,
        conversion_digest: input.conversion_digest.clone(),
    })
}
pub(crate) fn reserve_native_sql(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    campaign: &str,
    input: &crate::risk::ReserveInput,
    census: &crate::risk::VerifiedCensus,
) -> StoreResult<super::UnifiedFinanceWriteReceipt> {
    let receipt = native_reservation_receipt_sql(tx, installation, campaign, input)?;
    crate::risk::reserve_unified_native(tx, installation, campaign, input, census, &receipt)
        .map(super::UnifiedFinanceWriteReceipt::Risk)
        .map_err(StoreError::Invalid)
}

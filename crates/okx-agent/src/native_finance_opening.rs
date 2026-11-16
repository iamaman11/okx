//! Internal handoff from the actual TradeService producer, never public wire flags.
use crate::native_finance_risk::NativeFinanceRisk;
use okx_protocol::risk_authority::*;
impl NativeFinanceRisk {
    pub(crate) async fn install_produced_opening(
        &self,
        packet: crate::trade_service::ProducedOpening,
    ) -> Result<RiskResult, &'static str> {
        let (command, census) = packet.into_parts();
        if !command.valid() || !census.valid() || command.installation_id != self.installation {
            return Err("NATIVE_OPENING_PRODUCER_INVALID");
        }
        let RiskCommandAction::InstallEntryOpening { source } = &command.action else {
            return Err("NATIVE_OPENING_COMMAND_REQUIRED");
        };
        if source.campaign_id != self.campaign
            || source
                .accounts
                .iter()
                .map(|a| &a.account_key)
                .ne(census.account_keys.iter())
        {
            return Err("NATIVE_OPENING_PRODUCER_SCOPE_MISMATCH");
        }
        use okx_execution::account_worker::{
            NativeOpeningAccount, NativeOpeningSource, ReportedOpeningCensus,
        };
        let native = NativeOpeningSource {
            campaign_id: source.campaign_id.clone(),
            limit_usd: source.limit_usd.clone(),
            consumed_usd: source.consumed_usd.clone(),
            source_digest: source.source_digest.clone(),
            as_of_ms: source.as_of_ms,
            campaign_started_at_ms: source.campaign_started_at_ms,
            accounts: source
                .accounts
                .iter()
                .map(|a| NativeOpeningAccount {
                    account_key: a.account_key.clone(),
                    history_from_ms: a.history_from_ms,
                    history_through_ms: a.history_through_ms,
                    consumed_entry_usd: a.consumed_entry_usd.clone(),
                    order_history_complete: a.order_history_complete,
                    fill_history_complete: a.fill_history_complete,
                    bill_history_complete: a.bill_history_complete,
                    finance_provenance_sealed: a.finance_provenance_sealed,
                    source_digest: a.source_digest.clone(),
                })
                .collect(),
        };
        let reported = ReportedOpeningCensus {
            source_digest: census.source_digest,
            generated_at_ms: i64::try_from(census.generated_at_ms)
                .map_err(|_| "NATIVE_OPENING_TIME_INVALID")?,
            read_floor_ms: i64::try_from(census.source_read_floor_ms)
                .map_err(|_| "NATIVE_OPENING_TIME_INVALID")?,
            occupied: census
                .occupied
                .iter()
                .map(|a| (a.account_key.clone(), a.occupied_usdt.clone()))
                .collect(),
            cumulative_loss_usd: census
                .occupied
                .iter()
                .map(|a| (a.account_key.clone(), a.cumulative_loss_usd.clone()))
                .collect(),
        };
        let committed = self
            .owner
            .install_reported_opening(native, reported)
            .await
            .map_err(|e| match e {
                okx_execution::StoreError::Invalid(reason) => reason,
                _ => "NATIVE_OPENING_OUTCOME_UNKNOWN",
            })?;
        if committed.source_digest != source.source_digest {
            return Err("NATIVE_OPENING_RECEIPT_MISMATCH");
        }
        let mut result = self
            .request(RiskCommand {
                action: RiskCommandAction::Summary {},
                ..command
            })
            .await?;
        if result
            .budget
            .as_ref()
            .and_then(|b| b.entry_budget.as_ref())
            .is_none_or(|b| b.opening_source_digest != committed.source_digest)
        {
            return Err("NATIVE_OPENING_READBACK_UNPROVEN");
        }
        result.status = RiskDecisionStatus::Observed;
        Ok(result)
    }
}

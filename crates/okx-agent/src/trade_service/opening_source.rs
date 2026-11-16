//! Only this TradeService producer can construct the internal opening packet.
use super::*;
pub(crate) struct ProducedOpening {
    command: RiskCommand,
    census: NativeRiskCensus,
}
impl TradeService {
    pub(super) async fn produce_opening(
        &self,
        state: &CentralGovernanceSnapshot,
        source: CampaignOpeningSource,
    ) -> Result<ProducedOpening, String> {
        let census = self.census(state).await?;
        let command = self.risk_command(
            state,
            format!("install_opening_{}", now()),
            RiskCommandAction::InstallEntryOpening { source },
        );
        if !command.valid() || !census.valid() {
            return Err("ENTRY_OPENING_LIVE_CENSUS_INCOMPLETE".into());
        }
        Ok(ProducedOpening { command, census })
    }
}
impl ProducedOpening {
    pub(crate) fn into_parts(self) -> (RiskCommand, NativeRiskCensus) {
        (self.command, self.census)
    }
}

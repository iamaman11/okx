//! One local financial authority; Cloudflare is authenticated delivery only.
//! A stopped owner supports inspection and never grants write permission.
use crate::native_finance_risk::{NativeFinanceRisk, NativePolicyObservation};
use okx_execution::account_worker::UnifiedFinanceHandle;
use okx_protocol::account_governance::CentralGovernanceSnapshot;
use okx_protocol::risk_authority::{RiskCommand, RiskResult};

/// No constructor can route financial commands to a remote worker.
/// ```compile_fail,E0599
/// let _ = okx_agent::risk_bridge::NativeRiskClient::new;
/// ```
#[derive(Clone)]
pub struct NativeRiskClient {
    backend: NativeFinanceRisk,
    mode: FinanceMode,
}
#[derive(Clone, Copy)]
enum FinanceMode {
    Active,
    Stopped,
}

impl NativeRiskClient {
    pub(crate) fn finance_owner(&self) -> UnifiedFinanceHandle {
        self.backend.owner.clone()
    }
    pub fn local(backend: NativeFinanceRisk) -> Self {
        Self {
            backend,
            mode: FinanceMode::Active,
        }
    }
    pub fn local_stopped(backend: NativeFinanceRisk) -> Self {
        Self {
            backend,
            mode: FinanceMode::Stopped,
        }
    }
    pub(crate) async fn reserve_produced(
        &self,
        packet: crate::trade_service::ProducedReserve,
    ) -> Result<(RiskResult, Option<okx_execution::CentralReceipt>), &'static str> {
        self.backend
            .resolved()
            .await?
            .reserve_produced(packet)
            .await
    }
    pub(crate) async fn post_produced(
        &self,
        packet: crate::trade_service::ProducedPost,
    ) -> Result<(RiskResult, Option<okx_execution::CommittedVenueSend>), &'static str> {
        self.backend.resolved().await?.post_produced(packet).await
    }
    pub(crate) async fn install_produced_opening(
        &self,
        packet: crate::trade_service::ProducedOpening,
    ) -> Result<RiskResult, &'static str> {
        self.backend
            .resolved()
            .await?
            .install_produced_opening(packet)
            .await
    }
    /// Observing host policy and granting a financial operation are distinct.
    pub async fn observe_required_governance(
        &self,
        snapshot: &CentralGovernanceSnapshot,
    ) -> Result<NativePolicyObservation, String> {
        match self.mode {
            FinanceMode::Active => self.backend.observe_governance(snapshot).await,
            FinanceMode::Stopped => {
                self.backend.verify_stopped_observation(snapshot).await?;
                Ok(NativePolicyObservation::Applied)
            }
        }
    }
    pub async fn apply_required_governance(
        &self,
        snapshot: &CentralGovernanceSnapshot,
    ) -> Result<(), String> {
        match self.mode {
            FinanceMode::Active => self.backend.apply_governance(snapshot).await,
            FinanceMode::Stopped => self.backend.verify_stopped_observation(snapshot).await,
        }
    }
    pub async fn request(&self, command: RiskCommand) -> Result<RiskResult, &'static str> {
        if !command.valid() {
            return Err("INVALID_RISK_COMMAND");
        }
        self.backend.resolved().await?.request(command).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_protocol::risk_authority::{RISK_COMMAND_SCHEMA_V1, RiskCommandAction};

    #[tokio::test]
    async fn invalid_command_never_enters_the_financial_owner() {
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce).unwrap();
        let root = std::env::temp_dir().join(format!(
            "okx-local-risk-{:032x}",
            u128::from_le_bytes(nonce)
        ));
        std::fs::create_dir(&root).unwrap();
        let owner = UnifiedFinanceHandle::initialize_new(
            root.clone(),
            okx_execution::NewFinanceInstallation {
                installation_id: "installation-local-risk".into(),
                initialize_request_id: "initialize_local_risk_001".into(),
                source_tree: "a".repeat(40),
            },
        )
        .await
        .unwrap();
        let client = NativeRiskClient::local(NativeFinanceRisk::from_owner(owner.clone()));
        assert_eq!(
            client.finance_owner().installation_id(),
            owner.installation_id()
        );
        let jobs = owner.sql_timing().observed_jobs;
        let result = client
            .request(RiskCommand {
                schema: RISK_COMMAND_SCHEMA_V1.into(),
                command_id: String::new(),
                installation_id: owner.installation_id().into(),
                action: RiskCommandAction::Summary {},
            })
            .await;
        assert_eq!(result.err(), Some("INVALID_RISK_COMMAND"));
        assert_eq!(owner.sql_timing().observed_jobs, jobs);
        let mut terminated = owner.termination_signal();
        drop(client);
        drop(owner);
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            terminated.wait_for(|v| *v),
        )
        .await
        .unwrap()
        .unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
